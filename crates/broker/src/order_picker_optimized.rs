// Copyright (c) 2025 RISC Zero, Inc.
//
// All rights reserved.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use crate::{
    chain_monitor::ChainMonitorService,
    config::{ConfigLock, OrderPricingPriority},
    db::DbObj,
    errors::CodedError,
    flashblocks_utils::FlashblocksProviderExt,
    FulfillmentType,
    provers::ProverObj,
    task::{RetryRes, RetryTask, SupervisorErr},
    utils, OrderRequest,
};
use crate::{now_timestamp, provers::ProofResult};
use alloy::{
    network::Ethereum,
    primitives::{
        utils::{format_ether, format_units, parse_ether},
        Address, U256,
    },
    providers::{Provider, WalletProvider},
    uint,
};
use anyhow::{Context, Result};
use boundless_market::{
    contracts::{boundless_market::BoundlessMarketService, RequestError},
    selector::SupportedSelectors,
};
use moka::future::Cache;
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use OrderPricingOutcome::{Lock, ProveAfterLockExpire, Skip};

const MIN_CAPACITY_CHECK_INTERVAL: Duration = Duration::from_secs(5);

const ONE_MILLION: U256 = uint!(1_000_000_U256);

/// Maximum number of orders to cache for deduplication
const ORDER_DEDUP_CACHE_SIZE: u64 = 1000;

/// In-memory LRU cache for order deduplication by ID (prevents duplicate order processing)
type OrderCache = Arc<Cache<String, ()>>;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum OrderPickerErr {
    #[error("{code} failed to fetch / push input: {0}", code = self.code())]
    FetchInputErr(#[source] anyhow::Error),

    #[error("{code} failed to fetch / push image: {0}", code = self.code())]
    FetchImageErr(#[source] anyhow::Error),

    #[error("{code} guest panicked: {0}", code = self.code())]
    GuestPanic(String),

    #[error("{code} invalid request: {0}", code = self.code())]
    RequestError(#[from] RequestError),

    #[error("{code} RPC error: {0:?}", code = self.code())]
    RpcErr(anyhow::Error),

    #[error("{code} Unexpected error: {0:?}", code = self.code())]
    UnexpectedErr(#[from] anyhow::Error),
}

impl CodedError for OrderPickerErr {
    fn code(&self) -> &str {
        match self {
            OrderPickerErr::FetchInputErr(_) => "[B-OP-001]",
            OrderPickerErr::FetchImageErr(_) => "[B-OP-002]",
            OrderPickerErr::GuestPanic(_) => "[B-OP-003]",
            OrderPickerErr::RequestError(_) => "[B-OP-004]",
            OrderPickerErr::RpcErr(_) => "[B-OP-005]",
            OrderPickerErr::UnexpectedErr(_) => "[B-OP-500]",
        }
    }
}

#[derive(Clone)]
pub struct OrderPicker<P> {
    db: DbObj,
    config: ConfigLock,
    prover: ProverObj,
    provider: Arc<P>,
    chain_monitor: Arc<ChainMonitorService<P>>,
    market: BoundlessMarketService<Arc<P>>,
    supported_selectors: SupportedSelectors,
    // TODO ideal not to wrap in mutex, but otherwise would require supervisor refactor, try to find alternative
    new_order_rx: Arc<Mutex<mpsc::Receiver<Box<OrderRequest>>>>,
    priced_orders_tx: mpsc::Sender<Box<OrderRequest>>,
    stake_token_decimals: u8,
    order_cache: OrderCache,
}

#[derive(Debug)]
#[non_exhaustive]
enum OrderPricingOutcome {
    // Order should be locked and proving commence after lock is secured
    Lock {
        total_cycles: u64,
        target_timestamp_secs: u64,
        // TODO handle checking what time the lock should occur before, when estimating proving time.
        expiry_secs: u64,
    },
    // Do not lock the order, but consider proving and fulfilling it after the lock expires
    ProveAfterLockExpire {
        total_cycles: u64,
        lock_expire_timestamp_secs: u64,
        expiry_secs: u64,
    },
    // Do not accept engage order
    Skip,
}

impl<P> OrderPicker<P>
where
    P: Provider<Ethereum> + 'static + Clone + WalletProvider,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db: DbObj,
        config: ConfigLock,
        prover: ProverObj,
        market_addr: Address,
        provider: Arc<P>,
        chain_monitor: Arc<ChainMonitorService<P>>,
        new_order_rx: mpsc::Receiver<Box<OrderRequest>>,
        order_result_tx: mpsc::Sender<Box<OrderRequest>>,
        stake_token_decimals: u8,
    ) -> Self {
        let market = BoundlessMarketService::new(
            market_addr,
            provider.clone(),
            provider.default_signer_address(),
        );

        Self {
            db,
            config,
            prover,
            provider,
            chain_monitor,
            market,
            supported_selectors: SupportedSelectors::default(),
            new_order_rx: Arc::new(Mutex::new(new_order_rx)),
            priced_orders_tx: order_result_tx,
            stake_token_decimals,
            order_cache: Arc::new(
                Cache::builder()
                    .max_capacity(ORDER_DEDUP_CACHE_SIZE)
                    .time_to_live(Duration::from_secs(30 * 60)) // 30 minutes
                    .build(),
            ),
        }
    }

    async fn price_order_and_update_state(
        &self,
        mut order: Box<OrderRequest>,
        cancel_token: CancellationToken,
    ) -> bool {
        let order_id = order.id();
        let f = || async {
            let pricing_result = tokio::select! {
                result = self.price_order(&mut order) => result,
                _ = cancel_token.cancelled() => {
                    tracing::debug!("Order pricing cancelled during pricing for order {order_id}");
                    return Ok(false);
                }
            };

            match pricing_result {
                Ok(Lock { total_cycles, target_timestamp_secs, expiry_secs }) => {
                    order.total_cycles = Some(total_cycles);
                    order.target_timestamp = Some(target_timestamp_secs);
                    order.expire_timestamp = Some(expiry_secs);

                    tracing::info!(
                        "Order {order_id} scheduled for lock attempt in {}s (timestamp: {}), when price threshold met",
                        target_timestamp_secs.saturating_sub(now_timestamp()),
                        target_timestamp_secs,
                    );

                    self.priced_orders_tx
                        .send(order)
                        .await
                        .context("Failed to send to order_result_tx")?;

                    Ok::<_, OrderPickerErr>(true)
                }
                Ok(ProveAfterLockExpire {
                    total_cycles,
                    lock_expire_timestamp_secs,
                    expiry_secs,
                }) => {
                    tracing::info!("Setting order {order_id} to prove after lock expiry at {lock_expire_timestamp_secs}");
                    order.total_cycles = Some(total_cycles);
                    order.target_timestamp = Some(lock_expire_timestamp_secs);
                    order.expire_timestamp = Some(expiry_secs);

                    self.priced_orders_tx
                        .send(order)
                        .await
                        .context("Failed to send to order_result_tx")?;

                    Ok(true)
                }
                Ok(Skip) => {
                    tracing::info!("Skipping order {order_id}");

                    // Add the skipped order to the database
                    self.db
                        .insert_skipped_request(&order)
                        .await
                        .context("Failed to add skipped order to database")?;
                    Ok(false)
                }
                Err(err) => {
                    tracing::warn!("Failed to price order {order_id}: {err}");
                    self.db
                        .insert_skipped_request(&order)
                        .await
                        .context("Failed to skip failed priced order")?;
                    Ok(false)
                }
            }
        };

        match f().await {
            Ok(true) => true,
            Ok(false) => false,
            Err(err) => {
                tracing::error!("Failed to update for order {order_id}: {err}");
                false
            }
        }
    }

    async fn price_order(
        &self,
        order: &mut OrderRequest,
    ) -> Result<OrderPricingOutcome, OrderPickerErr> {
        let order_id = order.id();
        tracing::debug!("Pricing order {order_id}");

        // Short circuit if the order has been locked.
        if order.fulfillment_type == FulfillmentType::LockAndFulfill
            && self
                .db
                .is_request_locked(U256::from(order.request.id))
                .await
                .context("Failed to check if request is locked before pricing")?
        {
            tracing::debug!("Order {order_id} is already locked, skipping");
            return Ok(Skip);
        }

        let (min_deadline, allowed_addresses_opt, denied_addresses_opt) = {
            let config = self.config.lock_all().context("Failed to read config")?;
            (
                config.market.min_deadline,
                config.market.allow_client_addresses.clone(),
                config.market.deny_requestor_addresses.clone(),
            )
        };

        // Initial sanity checks:
        if let Some(allow_addresses) = allowed_addresses_opt {
            let client_addr = order.request.client_address();
            if !allow_addresses.contains(&client_addr) {
                tracing::info!("Removing order {order_id} from {client_addr} because it is not in allowed addrs");
                return Ok(Skip);
            }
        }

        if let Some(deny_addresses) = denied_addresses_opt {
            let client_addr = order.request.client_address();
            if deny_addresses.contains(&client_addr) {
                tracing::info!(
                    "Removing order {order_id} from {client_addr} because it is in denied addrs"
                );
                return Ok(Skip);
            }
        }

        if !self.supported_selectors.is_supported(order.request.requirements.selector) {
            tracing::info!(
                "Removing order {order_id} because it has an unsupported selector requirement"
            );

            return Ok(Skip);
        };

        // Lock expiration is the timestamp before which the order must be filled in order to avoid slashing
        let lock_expiration =
            order.request.offer.biddingStart + order.request.offer.lockTimeout as u64;
        // order expiration is the timestamp after which the order can no longer be filled by anyone.
        let order_expiration =
            order.request.offer.biddingStart + order.request.offer.timeout as u64;

        let now = now_timestamp();

        // If order_expiration > lock_expiration the period in-between is when order can be filled
        // by anyone without staking to partially claim the slashed stake
        let lock_expired = order.fulfillment_type == FulfillmentType::FulfillAfterLockExpire;

        let (expiration, lockin_stake) = if lock_expired {
            (order_expiration, U256::ZERO)
        } else {
            (lock_expiration, U256::from(order.request.offer.lockStake))
        };

        if expiration <= now {
            tracing::info!("Removing order {order_id} because it has expired");
            return Ok(Skip);
        };

        // Does the order expire within the min deadline
        let seconds_left = expiration.saturating_sub(now);
        if seconds_left <= min_deadline {
            tracing::info!("Removing order {order_id} because it expires within min_deadline: {seconds_left}, min_deadline: {min_deadline}");
            return Ok(Skip);
        }

        // Check if the stake is sane and if we can afford it
        // For lock expired orders, we don't check the max stake because we can't lock those orders.
        let max_stake = {
            let config = self.config.lock_all().context("Failed to read config")?;
            parse_ether(&config.market.max_stake).context("Failed to parse max_stake")?
        };

        if !lock_expired && lockin_stake > max_stake {
            tracing::info!("Removing high stake order {order_id}, lock stake: {lockin_stake}, max stake: {max_stake}");
            return Ok(Skip);
        }

        // Check that we have both enough staking tokens to stake, and enough gas tokens to lock and fulfil
        // NOTE: We use the current gas price and a rough heuristic on gas costs. Its possible that
        // gas prices may go up (or down) by the time its time to fulfill. This does not aim to be
        // a tight estimate, although improving this estimate will allow for a more profit.
        let gas_price =
            self.chain_monitor.current_gas_price().await.context("Failed to get gas price")?;
        let order_gas = if lock_expired {
            // No need to include lock gas if its a lock expired order
            U256::from(
                utils::estimate_gas_to_fulfill(
                    &self.config,
                    &self.supported_selectors,
                    &order.request,
                )
                .await?,
            )
        } else {
            U256::from(
                utils::estimate_gas_to_lock(&self.config, order).await?
                    + utils::estimate_gas_to_fulfill(
                        &self.config,
                        &self.supported_selectors,
                        &order.request,
                    )
                    .await?,
            )
        };
        let order_gas_cost = U256::from(gas_price) * order_gas;
        let available_gas = self.available_gas_balance().await?;
        let available_stake = self.available_stake_balance().await?;
        tracing::debug!(
            "Estimated {order_gas} gas to {} order {order_id}; {} ether @ {} gwei",
            if lock_expired { "fulfill" } else { "lock and fulfill" },
            format_ether(order_gas_cost),
            format_units(gas_price, "gwei").unwrap()
        );

        // 跳过gas成本检查，因为我们现在跳过preflight并直接锁定
        tracing::debug!(
            "Skipping gas cost check for order {order_id} - gas cost: {}, max price: {}",
            format_ether(order_gas_cost),
            format_ether(order.request.offer.maxPrice)
        );

        if order_gas_cost > available_gas {
            tracing::warn!("Estimated there will be insufficient gas for order {order_id} after locking and fulfilling pending orders; available_gas {} ether", format_ether(available_gas));
            return Ok(Skip);
        }

        if !lock_expired && lockin_stake > available_stake {
            tracing::warn!(
                "Insufficient available stake to lock order {order_id}. Requires {lockin_stake}, has {available_stake}"
            );
            return Ok(Skip);
        }

        let (_max_mcycle_limit, _peak_prove_khz) = {
            let config = self.config.lock_all().context("Failed to read config")?;
            (config.market.max_mcycle_limit, config.market.peak_prove_khz)
        };

        // ===== 完全跳过preflight执行 =====
        tracing::info!("SKIPPING PREFLIGHT for order {order_id} - proceeding directly to lock");
        
        // 使用估算的周期数，避免preflight执行
        let estimated_cycles = 1_000_000; // 1 mcycle估算
        
        // 创建假的proof_res，跳过preflight
        let proof_res = ProofResult {
            id: format!("skipped_preflight_{}", order_id),
            stats: crate::provers::ExecutorResp {
                segments: 1,
                user_cycles: estimated_cycles,
                total_cycles: estimated_cycles,
                assumption_count: 0,
            },
            elapsed_time: 0.0,
        };

        // 跳过journal大小检查
        tracing::debug!("Skipping journal size check for order {order_id}");
        
        // 跳过predicate验证（可选，建议保留）
        // 如果需要跳过predicate验证，可以注释掉下面的代码
        /*
        if !order.request.requirements.predicate.eval(journal.clone()) {
            tracing::info!("Order {order_id} predicate check failed, skipping");
            return Ok(Skip);
        }
        */

        self.evaluate_order(order, &proof_res, order_gas_cost, lock_expired).await
    }

    async fn evaluate_order(
        &self,
        order: &OrderRequest,
        proof_res: &ProofResult,
        order_gas_cost: U256,
        lock_expired: bool,
    ) -> Result<OrderPricingOutcome, OrderPickerErr> {
        if lock_expired {
            return self.evaluate_lock_expired_order(order, proof_res).await;
        } else {
            self.evaluate_lockable_order(order, proof_res, order_gas_cost).await
        }
    }

    /// Evaluate if a regular lockable order is worth picking based on the price and the configured min mcycle price
    async fn evaluate_lockable_order(
        &self,
        order: &OrderRequest,
        proof_res: &ProofResult,
        order_gas_cost: U256,
    ) -> Result<OrderPricingOutcome, OrderPickerErr> {
        let order_id = order.id();
        
        tracing::info!(
            "Order {order_id} - IMMEDIATE LOCK (PREFLIGHT SKIPPED): price {} ETH, estimated cycles: {}, gas cost: {} ETH",
            format_ether(U256::from(order.request.offer.minPrice)),
            proof_res.stats.total_cycles,
            format_ether(order_gas_cost),
        );

        // 立即锁定，不进行价格判断
        let target_timestamp_secs = 0; // 立即锁定
        let expiry_secs = order.request.offer.biddingStart + order.request.offer.lockTimeout as u64;

        Ok(Lock { total_cycles: proof_res.stats.total_cycles, target_timestamp_secs, expiry_secs })
    }

    /// Evaluate if a lock expired order is worth picking based on how much of the slashed stake token we can recover
    /// and the configured min mcycle price in stake tokens
    async fn evaluate_lock_expired_order(
        &self,
        order: &OrderRequest,
        proof_res: &ProofResult,
    ) -> Result<OrderPricingOutcome, OrderPickerErr> {
        let order_id = order.id();

        tracing::info!(
            "Order {order_id} - IMMEDIATE PROVE AFTER LOCK EXPIRE (PREFLIGHT SKIPPED): estimated cycles: {}",
            proof_res.stats.total_cycles,
        );

        // 立即处理过期订单，不进行价格判断
        Ok(ProveAfterLockExpire {
            total_cycles: proof_res.stats.total_cycles,
            lock_expire_timestamp_secs: order.request.offer.biddingStart
                + order.request.offer.lockTimeout as u64,
            expiry_secs: order.request.offer.biddingStart + order.request.offer.timeout as u64,
        })
    }

    /// Estimate of gas for fulfilling any orders either pending lock or locked
    async fn estimate_gas_to_fulfill_pending(&self) -> Result<u64> {
        let mut gas = 0;
        for order in self.db.get_committed_orders().await? {
            let gas_estimate = utils::estimate_gas_to_fulfill(
                &self.config,
                &self.supported_selectors,
                &order.request,
            )
            .await?;
            gas += gas_estimate;
        }
        tracing::debug!("Total gas estimate to fulfill pending orders: {}", gas);
        Ok(gas)
    }

    /// Estimate the total gas tokens reserved to lock and fulfill all pending orders
    async fn gas_balance_reserved(&self) -> Result<U256> {
        let gas_price =
            self.chain_monitor.current_gas_price().await.context("Failed to get gas price")?;
        let fulfill_pending_gas = self.estimate_gas_to_fulfill_pending().await?;
        Ok(U256::from(gas_price) * U256::from(fulfill_pending_gas))
    }

    /// Return available gas balance.
    ///
    /// This is defined as the balance of the signer account.
    async fn available_gas_balance(&self) -> Result<U256, OrderPickerErr> {
        let balance = self
            .provider
            .get_balance_flashblocks(self.provider.default_signer_address(), &self.config)
            .await
            .map_err(|err| OrderPickerErr::RpcErr(err.into()))?;

        let gas_balance_reserved = self.gas_balance_reserved().await?;

        let available = balance.saturating_sub(gas_balance_reserved);
        tracing::debug!(
            "available gas balance: (account_balance) {} - (expected_future_gas) {} = {}",
            format_ether(balance),
            format_ether(gas_balance_reserved),
            format_ether(available)
        );

        Ok(available)
    }

    /// Return available stake balance.
    ///
    /// This is defined as the balance in staking tokens of the signer account minus any pending locked stake.
    async fn available_stake_balance(&self) -> Result<U256> {
        let balance = self.market.balance_of_stake(self.provider.default_signer_address()).await?;
        Ok(balance)
    }
}

impl<P> RetryTask for OrderPicker<P>
where
    P: Provider<Ethereum> + 'static + Clone + WalletProvider,
{
    type Error = OrderPickerErr;
    fn spawn(&self, cancel_token: CancellationToken) -> RetryRes<Self::Error> {
        let picker = self.clone();

        Box::pin(async move {
            tracing::info!("Starting order picking monitor");

            let read_config = || -> Result<(usize, OrderPricingPriority), Self::Error> {
                let cfg = picker.config.lock_all().map_err(|err| {
                    OrderPickerErr::UnexpectedErr(anyhow::anyhow!("Failed to read config: {err}"))
                })?;
                Ok((
                    cfg.market.max_concurrent_preflights as usize,
                    cfg.market.order_pricing_priority,
                ))
            };

            let (mut current_capacity, mut priority_mode) =
                read_config().map_err(SupervisorErr::Fault)?;
            let mut tasks: JoinSet<()> = JoinSet::new();
            let mut rx = picker.new_order_rx.lock().await;
            let mut capacity_check_interval = tokio::time::interval(MIN_CAPACITY_CHECK_INTERVAL);
            let mut pending_orders: VecDeque<Box<OrderRequest>> = VecDeque::new();

            loop {
                tokio::select! {
                    // This channel is cancellation safe, so it's fine to use in the select!
                    Some(order) = rx.recv() => {
                        tracing::debug!("Queued order {} to be priced", order.id());
                        pending_orders.push_back(order);
                    }
                    _ = tasks.join_next(), if !tasks.is_empty() => {
                        tracing::trace!("Pricing task completed ({} remaining)", tasks.len());
                    }
                    _ = capacity_check_interval.tick() => {
                        // Check capacity on an interval for capacity changes in config
                        let (new_capacity, new_priority_mode) = read_config().map_err(SupervisorErr::Fault)?;
                        if new_capacity != current_capacity{
                            tracing::debug!("Pricing capacity changed from {} to {}", current_capacity, new_capacity);
                            current_capacity = new_capacity;
                        }
                        if new_priority_mode != priority_mode {
                            tracing::debug!("Order pricing priority changed from {:?} to {:?}", priority_mode, new_priority_mode);
                            priority_mode = new_priority_mode;
                        }
                    }
                    _ = cancel_token.cancelled() => {
                        tracing::debug!("Order picker received cancellation, shutting down gracefully");

                        // Wait for all pricing tasks to be cancelled gracefully
                        while tasks.join_next().await.is_some() {}
                        break;
                    }
                }

                // Process pending orders if we have capacity
                while !pending_orders.is_empty() && tasks.len() < current_capacity {
                    if let Some(order) =
                        picker.select_next_pricing_order(&mut pending_orders, priority_mode)
                    {
                        let order_id = order.id();

                        // Check if we've already started processing this order ID
                        if picker.order_cache.get(&order_id).await.is_some() {
                            tracing::debug!(
                                "Skipping duplicate order {order_id}, already being processed"
                            );
                            continue;
                        }

                        // Mark order as being processed immediately to prevent duplicates
                        picker.order_cache.insert(order_id.clone(), ()).await;

                        let picker_clone = picker.clone();
                        let task_cancel_token = cancel_token.child_token();
                        tasks.spawn(async move {
                            picker_clone
                                .price_order_and_update_state(order, task_cancel_token)
                                .await;
                        });
                    }
                }
            }
            Ok(())
        })
    }
}

/// Returns the maximum cycles that can be proven within a given time period
/// based on the proving rate provided, in khz.
fn calculate_max_cycles_for_time(prove_khz: u64, time_seconds: u64) -> u64 {
    (prove_khz.saturating_mul(1_000)).saturating_mul(time_seconds)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{
        chain_monitor::ChainMonitorService, db::SqliteDb, provers::DefaultProver, FulfillmentType,
        OrderStatus,
    };
    use alloy::{
        network::EthereumWallet,
        node_bindings::{Anvil, AnvilInstance},
        primitives::{address, aliases::U96, utils::parse_units, Address, Bytes, FixedBytes, B256},
        providers::{ext::AnvilApi, ProviderBuilder},
        signers::local::PrivateKeySigner,
    };
    use boundless_market::contracts::{
        Callback, Offer, Predicate, PredicateType, ProofRequest, RequestId, RequestInput,
        Requirements,
    };
    use boundless_market::storage::{MockStorageProvider, StorageProvider};
    use boundless_market_test_utils::{
        deploy_boundless_market, deploy_hit_points, ASSESSOR_GUEST_ID, ASSESSOR_GUEST_PATH,
        ECHO_ELF, ECHO_ID,
    };
    use risc0_ethereum_contracts::selector::Selector;
    use risc0_zkvm::sha::Digest;
    use tracing_test::traced_test;

    /// Reusable context for testing the order picker
    pub(crate) struct PickerTestCtx<P> {
        anvil: AnvilInstance,
        pub(crate) picker: OrderPicker<P>,
        boundless_market: BoundlessMarketService<Arc<P>>,
        storage_provider: MockStorageProvider,
        db: DbObj,
        provider: Arc<P>,
        priced_orders_rx: mpsc::Receiver<Box<OrderRequest>>,
        new_order_tx: mpsc::Sender<Box<OrderRequest>>,
    }

    /// Parameters for the generate_next_order function.
    pub(crate) struct OrderParams {
        pub(crate) order_index: u32,
        pub(crate) min_price: U256,
        pub(crate) max_price: U256,
        pub(crate) lock_stake: U256,
        pub(crate) fulfillment_type: FulfillmentType,
        pub(crate) bidding_start: u64,
        pub(crate) lock_timeout: u32,
        pub(crate) timeout: u32,
    }

    impl Default for OrderParams {
        fn default() -> Self {
            Self {
                order_index: 1,
                min_price: parse_ether("0.02").unwrap(),
                max_price: parse_ether("0.04").unwrap(),
                lock_stake: U256::ZERO,
                fulfillment_type: FulfillmentType::LockAndFulfill,
                bidding_start: now_timestamp(),
                lock_timeout: 900,
                timeout: 1200,
            }
        }
    }

    impl<P> PickerTestCtx<P>
    where
        P: Provider + WalletProvider,
    {
        pub(crate) fn signer(&self, index: usize) -> PrivateKeySigner {
            self.anvil.keys()[index].clone().into()
        }

        pub(crate) async fn generate_next_order(&self, params: OrderParams) -> Box<OrderRequest> {
            let image_url = self.storage_provider.upload_program(ECHO_ELF).await.unwrap();
            let image_id = Digest::from(ECHO_ID);
            let chain_id = self.provider.get_chain_id().await.unwrap();
            let boundless_market_address = self.boundless_market.instance().address();

            Box::new(OrderRequest {
                request: ProofRequest::new(
                    RequestId::new(self.provider.default_signer_address(), params.order_index),
                    Requirements::new(
                        image_id,
                        Predicate {
                            predicateType: PredicateType::PrefixMatch,
                            data: Default::default(),
                        },
                    ),
                    image_url,
                    RequestInput::builder()
                        .write_slice(&[0x41, 0x41, 0x41, 0x41])
                        .build_inline()
                        .unwrap(),
                    Offer {
                        minPrice: params.min_price,
                        maxPrice: params.max_price,
                        biddingStart: params.bidding_start,
                        timeout: params.timeout,
                        lockTimeout: params.lock_timeout,
                        rampUpPeriod: 1,
                        lockStake: params.lock_stake,
                    },
                ),
                target_timestamp: None,
                image_id: None,
                input_id: None,
                expire_timestamp: None,
                client_sig: Bytes::new(),
                fulfillment_type: params.fulfillment_type,
                boundless_market_address: *boundless_market_address,
                chain_id,
                total_cycles: None,
            })
        }
    }

    #[derive(Default)]
    pub(crate) struct PickerTestCtxBuilder {
        initial_signer_eth: Option<i32>,
        initial_hp: Option<U256>,
        config: Option<ConfigLock>,
        stake_token_decimals: Option<u8>,
    }

    impl PickerTestCtxBuilder {
        pub(crate) fn with_initial_signer_eth(self, eth: i32) -> Self {
            Self { initial_signer_eth: Some(eth), ..self }
        }
        pub(crate) fn with_initial_hp(self, hp: U256) -> Self {
            assert!(hp < U256::from(U96::MAX), "Cannot have more than 2^96 hit points");
            Self { initial_hp: Some(hp), ..self }
        }
        pub(crate) fn with_config(self, config: ConfigLock) -> Self {
            Self { config: Some(config), ..self }
        }
        pub(crate) fn with_stake_token_decimals(self, decimals: u8) -> Self {
            Self { stake_token_decimals: Some(decimals), ..self }
        }
        pub(crate) async fn build(
            self,
        ) -> PickerTestCtx<impl Provider + WalletProvider + Clone + 'static> {
            let anvil = Anvil::new()
                .args(["--balance", &format!("{}", self.initial_signer_eth.unwrap_or(10000))])
                .spawn();
            let signer: PrivateKeySigner = anvil.keys()[0].clone().into();
            let provider = Arc::new(
                ProviderBuilder::new()
                    .wallet(EthereumWallet::from(signer.clone()))
                    .connect(&anvil.endpoint())
                    .await
                    .unwrap(),
            );

            provider.anvil_mine(Some(4), Some(2)).await.unwrap();

            let hp_contract = deploy_hit_points(signer.address(), provider.clone()).await.unwrap();
            let market_address = deploy_boundless_market(
                signer.address(),
                provider.clone(),
                Address::ZERO,
                hp_contract,
                Digest::from(ASSESSOR_GUEST_ID),
                format!("file://{ASSESSOR_GUEST_PATH}"),
                Some(signer.address()),
            )
            .await
            .unwrap();

            let boundless_market = BoundlessMarketService::new(
                market_address,
                provider.clone(),
                provider.default_signer_address(),
            );

            if let Some(initial_hp) = self.initial_hp {
                tracing::debug!("Setting initial locked hitpoints to {}", initial_hp);
                boundless_market.deposit_stake_with_permit(initial_hp, &signer).await.unwrap();
                assert_eq!(
                    boundless_market
                        .balance_of_stake(provider.default_signer_address())
                        .await
                        .unwrap(),
                    initial_hp
                );
            }

            let storage_provider = MockStorageProvider::start();

            let db: DbObj = Arc::new(SqliteDb::new("sqlite::memory:").await.unwrap());
            let config = self.config.unwrap_or_default();
            let prover: ProverObj = Arc::new(DefaultProver::new());
            let chain_monitor = Arc::new(ChainMonitorService::new(provider.clone(), config.clone()).await.unwrap());
            tokio::spawn(chain_monitor.spawn(Default::default()));

            const TEST_CHANNEL_CAPACITY: usize = 50;
            let (_new_order_tx, new_order_rx) = mpsc::channel(TEST_CHANNEL_CAPACITY);
            let (priced_orders_tx, priced_orders_rx) = mpsc::channel(TEST_CHANNEL_CAPACITY);

            let picker = OrderPicker::new(
                db.clone(),
                config,
                prover,
                market_address,
                provider.clone(),
                chain_monitor,
                new_order_rx,
                priced_orders_tx,
                self.stake_token_decimals.unwrap_or(6),
            );

            PickerTestCtx {
                anvil,
                picker,
                boundless_market,
                storage_provider,
                db,
                provider,
                priced_orders_rx,
                new_order_tx: _new_order_tx,
            }
        }
    }

    #[tokio::test]
    #[traced_test]
    async fn price_order() {
        let config = ConfigLock::default();
        {
            config.load_write().unwrap().market.mcycle_price = "0.0000001".into();
        }
        let mut ctx = PickerTestCtxBuilder::default().with_config(config).build().await;

        let order = ctx.generate_next_order(Default::default()).await;

        let _request_id =
            ctx.boundless_market.submit_request(&order.request, &ctx.signer(0)).await.unwrap();

        let locked = ctx.picker.price_order_and_update_state(order, CancellationToken::new()).await;
        assert!(locked);

        let priced_order = ctx.priced_orders_rx.try_recv().unwrap();
        assert_eq!(priced_order.target_timestamp, Some(0));
    }

    #[tokio::test]
    #[traced_test]
    async fn skip_bad_predicate() {
        let config = ConfigLock::default();
        {
            config.load_write().unwrap().market.mcycle_price = "0.0000001".into();
        }
        let ctx = PickerTestCtxBuilder::default().with_config(config).build().await;

        let mut order = ctx.generate_next_order(Default::default()).await;
        // set a bad predicate
        order.request.requirements.predicate =
            Predicate { predicateType: PredicateType::DigestMatch, data: B256::ZERO.into() };

        let order_id = order.id();
        let _request_id =
            ctx.boundless_market.submit_request(&order.request, &ctx.signer(0)).await.unwrap();

        let locked = ctx.picker.price_order_and_update_state(order, CancellationToken::new()).await;
        assert!(!locked);

        let db_order = ctx.db.get_order(&order_id).await.unwrap().unwrap();
        assert_eq!(db_order.status, OrderStatus::Skipped);

        assert!(logs_contain("predicate check failed, skipping"));
    }

    #[tokio::test]
    #[traced_test]
    async fn skip_unsupported_selector() {
        let config = ConfigLock::default();
        {
            config.load_write().unwrap().market.mcycle_price = "0.0000001".into();
        }
        let ctx = PickerTestCtxBuilder::default().with_config(config).build().await;

        let mut order = ctx.generate_next_order(Default::default()).await;

        // set an unsupported selector
        order.request.requirements.selector = FixedBytes::from(Selector::Groth16V1_1 as u32);
        let order_id = order.id();

        let _request_id =
            ctx.boundless_market.submit_request(&order.request, &ctx.signer(0)).await.unwrap();

        let locked = ctx.picker.price_order_and_update_state(order, CancellationToken::new()).await;
        assert!(!locked);

        let db_order = ctx.db.get_order(&order_id).await.unwrap().unwrap();
        assert_eq!(db_order.status, OrderStatus::Skipped);

        assert!(logs_contain("has an unsupported selector requirement"));
    }

    #[tokio::test]
    #[traced_test]
    async fn skip_price_less_than_gas_costs() {
        let config = ConfigLock::default();
        {
            config.load_write().unwrap().market.mcycle_price = "0.0000001".into();
        }
        let ctx = PickerTestCtxBuilder::default().with_config(config).build().await;

        let order = ctx
            .generate_next_order(OrderParams {
                min_price: parse_ether("0.0005").unwrap(),
                max_price: parse_ether("0.0010").unwrap(),
                ..Default::default()
            })
            .await;
        let order_id = order.id();

        let _request_id =
            ctx.boundless_market.submit_request(&order.request, &ctx.signer(0)).await.unwrap();

        let locked = ctx.picker.price_order_and_update_state(order, CancellationToken::new()).await;
        assert!(!locked);

        let db_order = ctx.db.get_order(&order_id).await.unwrap().unwrap();
        assert_eq!(db_order.status, OrderStatus::Skipped);

        assert!(logs_contain(&format!("Estimated gas cost to lock and fulfill order {order_id}:")));
    }

    #[tokio::test]
    #[traced_test]
    async fn skip_price_less_than_gas_costs_groth16() {
        let config = ConfigLock::default();
        {
            config.load_write().unwrap().market.mcycle_price = "0.0000001".into();
        }
        let mut ctx = PickerTestCtxBuilder::default().with_config(config).build().await;

        // NOTE: Values currently adjusted ad hoc to be between the two thresholds.
        let min_price = parse_ether("0.0013").unwrap();
        let max_price = parse_ether("0.0013").unwrap();

        // Order should have high enough price with the default selector.
        let order = ctx
            .generate_next_order(OrderParams {
                order_index: 1,
                min_price,
                max_price,
                ..Default::default()
            })
            .await;

        let _request_id =
            ctx.boundless_market.submit_request(&order.request, &ctx.signer(0)).await.unwrap();

        let locked = ctx.picker.price_order_and_update_state(order, CancellationToken::new()).await;
        assert!(locked);
        let priced = ctx.priced_orders_rx.try_recv().unwrap();
        assert_eq!(priced.target_timestamp, Some(0));

        // Order does not have high enough price when groth16 is used.
        let mut order = ctx
            .generate_next_order(OrderParams {
                order_index: 2,
                min_price,
                max_price,
                ..Default::default()
            })
            .await;

        // set a Groth16 selector
        order.request.requirements.selector = FixedBytes::from(Selector::Groth16V2_1 as u32);

        let _request_id =
            ctx.boundless_market.submit_request(&order.request, &ctx.signer(0)).await.unwrap();

        let order_id = order.id();
        let locked = ctx.picker.price_order_and_update_state(order, CancellationToken::new()).await;
        assert!(!locked);

        let db_order = ctx.db.get_order(&order_id).await.unwrap().unwrap();
        assert_eq!(db_order.status, OrderStatus::Skipped);

        assert!(logs_contain(&format!("Estimated gas cost to lock and fulfill order {order_id}:")));
    }

    #[tokio::test]
    #[traced_test]
    async fn skip_price_less_than_gas_costs_callback() {
        let config = ConfigLock::default();
        {
            config.load_write().unwrap().market.mcycle_price = "0.0000001".into();
        }
        let mut ctx = PickerTestCtxBuilder::default().with_config(config).build().await;

        // NOTE: Values currently adjusted ad hoc to be between the two thresholds.
        let min_price = parse_ether("0.0013").unwrap();
        let max_price = parse_ether("0.0013").unwrap();

        // Order should have high enough price with the default selector.
        let order = ctx
            .generate_next_order(OrderParams {
                order_index: 1,
                min_price,
                max_price,
                ..Default::default()
            })
            .await;
        let _request_id =
            ctx.boundless_market.submit_request(&order.request, &ctx.signer(0)).await.unwrap();

        let locked = ctx.picker.price_order_and_update_state(order, CancellationToken::new()).await;
        assert!(locked);

        let priced = ctx.priced_orders_rx.try_recv().unwrap();
        assert_eq!(priced.target_timestamp, Some(0));

        // Order does not have high enough price when groth16 is used.
        let mut order = ctx
            .generate_next_order(OrderParams {
                order_index: 2,
                min_price,
                max_price,
                ..Default::default()
            })
            .await;

        // set a callback with a nontrivial gas consumption
        order.request.requirements.callback = Callback {
            addr: address!("0x00000000000000000000000000000000ca11bac2"),
            gasLimit: U96::from(200_000),
        };

        let _request_id =
            ctx.boundless_market.submit_request(&order.request, &ctx.signer(0)).await.unwrap();

        let order_id = order.id();
        let locked = ctx.picker.price_order_and_update_state(order, CancellationToken::new()).await;
        assert!(!locked);

        let db_order = ctx.db.get_order(&order_id).await.unwrap().unwrap();
        assert_eq!(db_order.status, OrderStatus::Skipped);

        assert!(logs_contain(&format!("Estimated gas cost to lock and fulfill order {order_id}:")));
    }

    #[tokio::test]
    #[traced_test]
    async fn skip_price_less_than_gas_costs_smart_contract_signature() {
        let config = ConfigLock::default();
        {
            config.load_write().unwrap().market.mcycle_price = "0.0000001".into();
        }
        let mut ctx = PickerTestCtxBuilder::default().with_config(config).build().await;

        // NOTE: Values currently adjusted ad hoc to be between the two thresholds.
        let min_price = parse_ether("0.0013").unwrap();
        let max_price = parse_ether("0.0013").unwrap();

        // Order should have high enough price with the default selector.
        let order = ctx
            .generate_next_order(OrderParams {
                order_index: 1,
                min_price,
                max_price,
                ..Default::default()
            })
            .await;

        let _request_id =
            ctx.boundless_market.submit_request(&order.request, &ctx.signer(0)).await.unwrap();

        let locked = ctx.picker.price_order_and_update_state(order, CancellationToken::new()).await;
        assert!(locked);

        let priced = ctx.priced_orders_rx.try_recv().unwrap();
        assert_eq!(priced.target_timestamp, Some(0));

        // Order does not have high enough price when groth16 is used.
        let mut order = ctx
            .generate_next_order(OrderParams {
                order_index: 2,
                min_price,
                max_price,
                ..Default::default()
            })
            .await;

        order.request.id =
            RequestId::try_from(order.request.id).unwrap().set_smart_contract_signed_flag().into();

        let _request_id =
            ctx.boundless_market.submit_request(&order.request, &ctx.signer(0)).await.unwrap();

        let order_id = order.id();
        let locked = ctx.picker.price_order_and_update_state(order, CancellationToken::new()).await;
        assert!(!locked);

        let db_order = ctx.db.get_order(&order_id).await.unwrap().unwrap();
        assert_eq!(db_order.status, OrderStatus::Skipped);

        assert!(logs_contain(&format!("Estimated gas cost to lock and fulfill order {order_id}:")));
    }

    #[tokio::test]
    #[traced_test]
    async fn skip_unallowed_addr() {
        let config = ConfigLock::default();
        {
            config.load_write().unwrap().market.mcycle_price = "0.0000001".into();
            config.load_write().unwrap().market.allow_client_addresses = Some(vec![Address::ZERO]);
        }
        let ctx = PickerTestCtxBuilder::default().with_config(config).build().await;

        let order = ctx.generate_next_order(Default::default()).await;

        let _request_id =
            ctx.boundless_market.submit_request(&order.request, &ctx.signer(0)).await.unwrap();

        let order_id = order.id();
        let locked = ctx.picker.price_order_and_update_state(order, CancellationToken::new()).await;
        assert!(!locked);

        let db_order = ctx.db.get_order(&order_id).await.unwrap().unwrap();
        assert_eq!(db_order.status, OrderStatus::Skipped);

        assert!(logs_contain("because it is not in allowed addrs"));
    }

    #[tokio::test]
    #[traced_test]
    async fn skip_denied_addr() {
        let config = ConfigLock::default();
        let ctx = PickerTestCtxBuilder::default().with_config(config.clone()).build().await;
        let deny_address = ctx.provider.default_signer_address();

        {
            let mut cfg = config.load_write().unwrap();
            cfg.market.mcycle_price = "0.0000001".into();
            cfg.market.deny_requestor_addresses = Some([deny_address].into_iter().collect());
        }

        let order = ctx.generate_next_order(Default::default()).await;

        let _request_id =
            ctx.boundless_market.submit_request(&order.request, &ctx.signer(0)).await.unwrap();

        let order_id = order.id();
        let locked = ctx.picker.price_order_and_update_state(order, CancellationToken::new()).await;
        assert!(!locked);

        let db_order = ctx.db.get_order(&order_id).await.unwrap().unwrap();
        assert_eq!(db_order.status, OrderStatus::Skipped);

        assert!(logs_contain("because it is in denied addrs"));
    }

    #[tokio::test]
    #[traced_test]
    async fn resume_order_pricing() {
        let config = ConfigLock::default();
        {
            config.load_write().unwrap().market.mcycle_price = "0.0000001".into();
        }
        let mut ctx = PickerTestCtxBuilder::default().with_config(config).build().await;

        let order = ctx.generate_next_order(Default::default()).await;
        let order_id = order.id();

        let _request_id =
            ctx.boundless_market.submit_request(&order.request, &ctx.signer(0)).await.unwrap();

        let pricing_task = tokio::spawn(ctx.picker.spawn(Default::default()));

        ctx.new_order_tx.send(order).await.unwrap();

        // Wait for the order to be priced, with some timeout
        let priced_order =
            tokio::time::timeout(Duration::from_secs(10), ctx.priced_orders_rx.recv())
                .await
                .unwrap();
        assert_eq!(priced_order.unwrap().id(), order_id);

        pricing_task.abort();

        // Send a new order when picker task is down.
        let new_order = ctx.generate_next_order(Default::default()).await;
        let new_order_id = new_order.id();
        ctx.new_order_tx.send(new_order).await.unwrap();

        assert!(ctx.priced_orders_rx.is_empty());

        tokio::spawn(ctx.picker.spawn(Default::default()));

        let priced_order =
            tokio::time::timeout(Duration::from_secs(10), ctx.priced_orders_rx.recv())
                .await
                .unwrap();
        assert_eq!(priced_order.unwrap().id(), new_order_id);
    }

    #[tokio::test]
    #[traced_test]
    async fn cannot_overcommit_stake() {
        let signer_inital_balance_eth = 2;
        let lockin_stake = U256::from(150);

        let config = ConfigLock::default();
        {
            config.load_write().unwrap().market.mcycle_price = "0.0000001".into();
            config.load_write().unwrap().market.max_stake = "10".into();
        }

        let mut ctx = PickerTestCtxBuilder::default()
            .with_initial_signer_eth(signer_inital_balance_eth)
            .with_initial_hp(lockin_stake)
            .with_config(config)
            .build()
            .await;
        let order = ctx
            .generate_next_order(OrderParams { lock_stake: U256::from(100), ..Default::default() })
            .await;
        let order1_id = order.id();
        assert!(ctx.picker.price_order_and_update_state(order, CancellationToken::new()).await);
        let priced = ctx.priced_orders_rx.try_recv().unwrap();
        assert_eq!(priced.id(), order1_id);

        let order = ctx
            .generate_next_order(OrderParams {
                lock_stake: lockin_stake + U256::from(1),
                ..Default::default()
            })
            .await;
        let order_id = order.id();
        assert!(!ctx.picker.price_order_and_update_state(order, CancellationToken::new()).await);
        assert!(logs_contain("Insufficient available stake to lock order"));
        assert_eq!(
            ctx.db.get_order(&order_id).await.unwrap().unwrap().status,
            OrderStatus::Skipped
        );

        let order = ctx
            .generate_next_order(OrderParams {
                lock_stake: parse_units("11", 18).unwrap().into(),
                ..Default::default()
            })
            .await;
        let order_id = order.id();
        assert!(!ctx.picker.price_order_and_update_state(order, CancellationToken::new()).await);

        // only the first order above should have marked as active pricing, the second one should have been skipped due to insufficient stake
        assert_eq!(
            ctx.db.get_order(&order_id).await.unwrap().unwrap().status,
            OrderStatus::Skipped
        );
        assert!(logs_contain("Removing high stake order"));
    }

    #[tokio::test]
    #[traced_test]
    async fn use_gas_to_fulfill_estimate_from_config() {
        let fulfill_gas = 123_456;
        let config = ConfigLock::default();
        {
            config.load_write().unwrap().market.mcycle_price = "0.0000001".into();
            config.load_write().unwrap().market.fulfill_gas_estimate = fulfill_gas;
        }

        let mut ctx = PickerTestCtxBuilder::default().with_config(config).build().await;

        let order = ctx.generate_next_order(Default::default()).await;
        let locked = ctx.picker.price_order_and_update_state(order, CancellationToken::new()).await;
        assert!(locked);

        // Simulate order being locked
        let order = ctx.priced_orders_rx.try_recv().unwrap();
        ctx.db.insert_accepted_request(&order, order.request.offer.minPrice).await.unwrap();

        assert_eq!(ctx.picker.estimate_gas_to_fulfill_pending().await.unwrap(), fulfill_gas);

        // add another order
        let order =
            ctx.generate_next_order(OrderParams { order_index: 2, ..Default::default() }).await;
        let locked = ctx.picker.price_order_and_update_state(order, CancellationToken::new()).await;
        assert!(locked);
        let order = ctx.priced_orders_rx.try_recv().unwrap();
        ctx.db.insert_accepted_request(&order, order.request.offer.minPrice).await.unwrap();

        // gas estimate stacks (until estimates factor in bundling)
        assert_eq!(ctx.picker.estimate_gas_to_fulfill_pending().await.unwrap(), 2 * fulfill_gas);
    }

    #[tokio::test]
    #[traced_test]
    async fn skips_journal_exceeding_limit() {
        // set this by testing a very small limit (1 byte)
        let config = ConfigLock::default();
        {
            config.load_write().unwrap().market.mcycle_price = "0.0000001".into();
            config.load_write().unwrap().market.max_journal_bytes = 1;
        }
        let lock_stake = U256::from(10);

        let ctx = PickerTestCtxBuilder::default()
            .with_config(config)
            .with_initial_hp(lock_stake)
            .build()
            .await;
        let order = ctx.generate_next_order(OrderParams { lock_stake, ..Default::default() }).await;

        let order_id = order.id();
        let locked = ctx.picker.price_order_and_update_state(order, CancellationToken::new()).await;
        assert!(!locked);

        assert_eq!(
            ctx.db.get_order(&order_id).await.unwrap().unwrap().status,
            OrderStatus::Skipped
        );
        assert!(logs_contain("journal larger than set limit"));
    }

    #[tokio::test]
    #[traced_test]
    async fn price_locked_by_other() {
        let config = ConfigLock::default();
        {
            config.load_write().unwrap().market.mcycle_price_stake_token = "0.0000001".into();
        }
        let mut ctx = PickerTestCtxBuilder::default()
            .with_config(config)
            .with_initial_hp(U256::from(1000))
            .build()
            .await;

        let order = ctx
            .generate_next_order(OrderParams {
                fulfillment_type: FulfillmentType::FulfillAfterLockExpire,
                bidding_start: now_timestamp(),
                lock_timeout: 1000,
                timeout: 10000,
                lock_stake: parse_units("0.1", 6).unwrap().into(),
                ..Default::default()
            })
            .await;

        let order_id = order.id();
        let expected_target_timestamp =
            order.request.offer.biddingStart + order.request.offer.lockTimeout as u64;
        let expected_expire_timestamp =
            order.request.offer.biddingStart + order.request.offer.timeout as u64;

        let expected_log = format!(
            "Setting order {} to prove after lock expiry at {}",
            order_id, expected_target_timestamp
        );
        assert!(ctx.picker.price_order_and_update_state(order, CancellationToken::new()).await);

        assert!(logs_contain(&expected_log));

        let priced = ctx.priced_orders_rx.try_recv().unwrap();
        assert_eq!(priced.target_timestamp, Some(expected_target_timestamp));
        assert_eq!(priced.expire_timestamp, Some(expected_expire_timestamp));
    }

    #[tokio::test]
    #[traced_test]
    async fn price_locked_by_other_unprofitable() {
        let config = ConfigLock::default();
        {
            config.load_write().unwrap().market.mcycle_price_stake_token = "0.1".into();
        }
        let ctx = PickerTestCtxBuilder::default()
            .with_stake_token_decimals(6)
            .with_config(config)
            .build()
            .await;

        let order = ctx
            .generate_next_order(OrderParams {
                fulfillment_type: FulfillmentType::FulfillAfterLockExpire,
                bidding_start: now_timestamp(),
                lock_timeout: 0,
                timeout: 10000,
                // Low stake means low reward for filling after it is unfulfilled
                lock_stake: parse_units("0.00001", 6).unwrap().into(),
                ..Default::default()
            })
            .await;

        let order_id = order.id();

        assert!(!ctx.picker.price_order_and_update_state(order, CancellationToken::new()).await);

        // Since we know the stake reward is constant, and we know our min_mycle_price_stake_token
        // the execution limit check tells us if the order is profitable or not, since it computes the max number
        // of cycles that can be proven while keeping the order profitable.
        assert!(logs_contain(&format!(
            "Skipping order {} due to session limit exceeded",
            order_id
        )));

        let db_order = ctx.db.get_order(&order_id).await.unwrap().unwrap();
        assert_eq!(db_order.status, OrderStatus::Skipped);
    }

    #[tokio::test]
    #[traced_test]
    async fn skip_mcycle_limit_for_allowed_address() {
        let exec_limit = 1000;
        let config = ConfigLock::default();
        {
            config.load_write().unwrap().market.mcycle_price = "0.0000001".into();
            config.load_write().unwrap().market.max_mcycle_limit = Some(exec_limit);
        }
        let ctx = PickerTestCtxBuilder::default().with_config(config).build().await;

        ctx.picker.config.load_write().as_mut().unwrap().market.priority_requestor_addresses =
            Some(vec![ctx.provider.default_signer_address()]);

        // First order from allowed address - should skip mcycle limit
        let order = ctx.generate_next_order(Default::default()).await;
        let order_id = order.id();

        let locked = ctx.picker.price_order_and_update_state(order, CancellationToken::new()).await;
        assert!(locked);

        // Check logs for the expected message about skipping mcycle limit
        assert!(logs_contain(&format!(
            "Order {order_id} exec limit skipped due to client {} being part of priority_requestor_addresses.",
            ctx.provider.default_signer_address()
        )));

        // Second order from a different address - should have mcycle limit enforced
        let mut order2 =
            ctx.generate_next_order(OrderParams { order_index: 2, ..Default::default() }).await;
        // Set a different client address
        order2.request.id = RequestId::new(Address::ZERO, 2).into();
        let order2_id = order2.id();

        let locked =
            ctx.picker.price_order_and_update_state(order2, CancellationToken::new()).await;
        assert!(locked);

        // Check logs for the expected message about setting exec limit to max_mcycle_limit
        assert!(logs_contain(&format!("Order {} exec limit computed from max price", order2_id)));
        assert!(logs_contain("exceeds config max_mcycle_limit"));
        assert!(logs_contain("setting exec limit to max_mcycle_limit"));
    }

    #[tokio::test]
    #[traced_test]
    async fn test_deadline_exec_limit_and_peak_prove_khz() {
        let config = ConfigLock::default();
        {
            config.load_write().unwrap().market.mcycle_price = "0.0000001".into();
            config.load_write().unwrap().market.peak_prove_khz = Some(1);
            config.load_write().unwrap().market.min_deadline = 10;
        }
        let ctx = PickerTestCtxBuilder::default().with_config(config).build().await;

        let order = ctx
            .generate_next_order(OrderParams {
                min_price: parse_ether("10").unwrap(),
                max_price: parse_ether("10").unwrap(),
                bidding_start: now_timestamp(),
                lock_timeout: 150,
                timeout: 300,
                ..Default::default()
            })
            .await;

        let order_id = order.id();
        let _submit_result =
            ctx.boundless_market.submit_request(&order.request, &ctx.signer(0)).await;

        let locked = ctx.picker.price_order_and_update_state(order, CancellationToken::new()).await;
        assert!(locked);

        let expected_log_pattern = format!("Order {order_id} preflight cycle limit adjusted to");
        assert!(logs_contain(&expected_log_pattern));
        assert!(logs_contain("capped by"));
        assert!(logs_contain("peak_prove_khz config"));
    }

    #[tokio::test]
    #[traced_test]
    async fn test_capacity_change() {
        let config = ConfigLock::default();
        {
            let mut cfg = config.load_write().unwrap();
            cfg.market.mcycle_price = "0.0000001".into();
            cfg.market.max_concurrent_preflights = 2;
        }
        let mut ctx = PickerTestCtxBuilder::default().with_config(config.clone()).build().await;

        // Start the order picker task
        let picker_task = tokio::spawn(ctx.picker.spawn(Default::default()));

        // Send an initial order to trigger the capacity check
        let order1 =
            ctx.generate_next_order(OrderParams { order_index: 1, ..Default::default() }).await;
        ctx.new_order_tx.send(order1).await.unwrap();

        // Wait for order to be processed
        tokio::time::timeout(Duration::from_secs(10), ctx.priced_orders_rx.recv()).await.unwrap();

        // Sleep to allow for a capacity check change
        tokio::time::sleep(MIN_CAPACITY_CHECK_INTERVAL).await;

        // Decrease capacity
        {
            let mut cfg = config.load_write().unwrap();
            cfg.market.max_concurrent_preflights = 1;
        }

        // Wait a bit more for the interval timer to fire and detect the change
        tokio::time::sleep(MIN_CAPACITY_CHECK_INTERVAL + Duration::from_millis(100)).await;

        // Send another order to trigger capacity check
        let order2 =
            ctx.generate_next_order(OrderParams { order_index: 2, ..Default::default() }).await;
        ctx.new_order_tx.send(order2).await.unwrap();

        // Wait for an order to be processed before updating capacity
        tokio::time::timeout(Duration::from_secs(10), ctx.priced_orders_rx.recv()).await.unwrap();

        // Check logs for capacity changes
        assert!(logs_contain("Pricing capacity changed from 2 to 1"));

        picker_task.abort();
    }

    #[tokio::test]
    #[traced_test]
    async fn test_lock_expired_exec_limit_precision_loss() {
        let config = ConfigLock::default();
        {
            config.load_write().unwrap().market.mcycle_price_stake_token = "1".into();
        }
        let ctx = PickerTestCtxBuilder::default()
            .with_config(config.clone())
            .with_stake_token_decimals(6)
            .build()
            .await;

        let mut order = ctx
            .generate_next_order(OrderParams {
                lock_stake: U256::from(4),
                fulfillment_type: FulfillmentType::FulfillAfterLockExpire,
                bidding_start: now_timestamp() - 100,
                lock_timeout: 10,
                timeout: 300,
                ..Default::default()
            })
            .await;

        let order_id = order.id();
        let stake_reward = order.request.offer.stake_reward_if_locked_and_not_fulfilled();
        assert_eq!(stake_reward, U256::from(1));

        let locked = ctx.picker.price_order(&mut order).await;
        assert!(matches!(locked, Ok(OrderPricingOutcome::Skip)));

        assert!(logs_contain(&format!(
            "Removing order {order_id} because its exec limit is too low"
        )));

        let mut order2 = ctx
            .generate_next_order(OrderParams {
                order_index: 2,
                lock_stake: U256::from(40),
                fulfillment_type: FulfillmentType::FulfillAfterLockExpire,
                bidding_start: now_timestamp() - 100,
                lock_timeout: 10,
                timeout: 300,
                ..Default::default()
            })
            .await;

        let order2_id = order2.id();
        let stake_reward2 = order2.request.offer.stake_reward_if_locked_and_not_fulfilled();
        assert_eq!(stake_reward2, U256::from(10));

        let locked = ctx.picker.price_order(&mut order2).await;
        assert!(matches!(locked, Ok(OrderPricingOutcome::Skip)));

        // Stake token denom offsets the mcycle multiplier, so for 1stake/mcycle, this will be 10
        assert!(logs_contain(&format!("exec limit cycles for order {order2_id}: 10")));
        assert!(logs_contain(&format!("Skipping order {order2_id} due to session limit exceeded")));
    }

    #[tokio::test]
    #[traced_test]
    async fn test_order_is_locked_check() -> Result<()> {
        let ctx = PickerTestCtxBuilder::default().build().await;

        let mut order = ctx.generate_next_order(Default::default()).await;
        let order_id = order.id();

        ctx.db
            .set_request_locked(
                U256::from(order.request.id),
                &ctx.provider.default_signer_address().to_string(),
                1000,
            )
            .await?;

        assert!(ctx.db.is_request_locked(U256::from(order.request.id)).await?);

        let pricing_outcome = ctx.picker.price_order(&mut order).await?;
        assert!(matches!(pricing_outcome, OrderPricingOutcome::Skip));

        assert!(logs_contain(&format!("Order {order_id} is already locked, skipping")));

        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_duplicate_order_cache() -> Result<()> {
        let mut ctx = PickerTestCtxBuilder::default().build().await;

        let order1 = ctx.generate_next_order(Default::default()).await;
        let order_id = order1.id();

        // Duplicate order
        let order2 = Box::new(OrderRequest {
            request: order1.request.clone(),
            client_sig: order1.client_sig.clone(),
            fulfillment_type: order1.fulfillment_type,
            boundless_market_address: order1.boundless_market_address,
            chain_id: order1.chain_id,
            image_id: order1.image_id.clone(),
            input_id: order1.input_id.clone(),
            total_cycles: order1.total_cycles,
            target_timestamp: order1.target_timestamp,
            expire_timestamp: order1.expire_timestamp,
        });

        assert_eq!(order1.id(), order2.id(), "Both orders should have the same ID");

        tokio::spawn(ctx.picker.spawn(CancellationToken::new()));

        ctx.new_order_tx.send(order1).await?;
        ctx.new_order_tx.send(order2).await?;

        let first_processed =
            tokio::time::timeout(Duration::from_secs(10), ctx.priced_orders_rx.recv())
                .await?
                .unwrap();

        assert_eq!(first_processed.id(), order_id, "First order should be processed");

        let second_result =
            tokio::time::timeout(Duration::from_secs(2), ctx.priced_orders_rx.recv()).await;

        assert!(second_result.is_err(), "Second order should be deduplicated and not processed");

        assert!(logs_contain(&format!(
            "Skipping duplicate order {order_id}, already being processed"
        )));

        Ok(())
    }
}