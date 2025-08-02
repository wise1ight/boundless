// Copyright 2025 RISC Zero, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::chain_monitor::ChainHead;
use crate::flashblocks_utils::FlashblocksProviderExt;
use crate::OrderRequest;
use crate::{
    chain_monitor::ChainMonitorService,
    config::{ConfigLock, OrderCommitmentPriority},
    db::DbObj,
    errors::CodedError,
    impl_coded_debug, now_timestamp,
    task::{RetryRes, RetryTask, SupervisorErr},
    utils, FulfillmentType, Order,
};
use alloy::{
    network::Ethereum,
    primitives::{
        utils::{format_ether, parse_units},
        Address, U256,
    },
    providers::{Provider, WalletProvider},
};
use anyhow::{Context, Result};
use boundless_market::contracts::{
    boundless_market::{BoundlessMarketService, MarketError},
    IBoundlessMarket::IBoundlessMarketErrors,
    RequestStatus, TxnErr,
};
use boundless_market::selector::SupportedSelectors;
use moka::{future::Cache, Expiry};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

/// Hard limit on the number of orders to concurrently kick off proving work for.
const MAX_PROVING_BATCH_SIZE: u32 = 10;

#[derive(Error)]
pub enum OrderMonitorErr {
    #[error("{code} Failed to lock order: {0}", code = self.code())]
    LockTxFailed(String),

    #[error("{code} Failed to confirm lock tx: {0}", code = self.code())]
    LockTxNotConfirmed(String),

    #[error("{code} Insufficient balance for lock", code = self.code())]
    InsufficientBalance,

    #[error("{code} Order already locked", code = self.code())]
    AlreadyLocked,

    #[error("{code} RPC error: {0:?}", code = self.code())]
    RpcErr(anyhow::Error),

    #[error("{code} Unexpected error: {0:?}", code = self.code())]
    UnexpectedError(#[from] anyhow::Error),
}

impl_coded_debug!(OrderMonitorErr);

impl CodedError for OrderMonitorErr {
    fn code(&self) -> &str {
        match self {
            OrderMonitorErr::LockTxNotConfirmed(_) => "[B-OM-006]",
            OrderMonitorErr::LockTxFailed(_) => "[B-OM-007]",
            OrderMonitorErr::AlreadyLocked => "[B-OM-009]",
            OrderMonitorErr::InsufficientBalance => "[B-OM-010]",
            OrderMonitorErr::RpcErr(_) => "[B-OM-011]",
            OrderMonitorErr::UnexpectedError(_) => "[B-OM-500]",
        }
    }
}

/// Represents the capacity for proving orders that we have available given our config.
/// Also manages vending out capacity for proving, preventing too many proofs from being
/// kicked off in each iteration.
#[derive(Debug, PartialEq)]
enum Capacity {
    /// There are orders that have been picked for proving but not fulfilled yet.
    /// Number indicates available slots.
    Available(u32),
    /// There is no concurrent lock limit.
    Unlimited,
}

impl Capacity {
    /// Returns the number of proofs we can kick off in the current iteration. Capped at
    /// [MAX_PROVING_BATCH_SIZE] to limit number of proving tasks spawned at once.
    fn request_capacity(&self, request: u32) -> u32 {
        match self {
            Capacity::Available(capacity) => {
                if request > *capacity {
                    std::cmp::min(*capacity, MAX_PROVING_BATCH_SIZE)
                } else {
                    std::cmp::min(request, MAX_PROVING_BATCH_SIZE)
                }
            }
            Capacity::Unlimited => std::cmp::min(MAX_PROVING_BATCH_SIZE, request),
        }
    }
}

struct OrderExpiry;

impl<K: std::hash::Hash + Eq, V: std::borrow::Borrow<OrderRequest>> Expiry<K, V> for OrderExpiry {
    fn expire_after_create(&self, _key: &K, value: &V, _now: Instant) -> Option<Duration> {
        let order: &OrderRequest = value.borrow();
        order.expire_timestamp.map(|t| {
            let time_until_expiry = t.saturating_sub(now_timestamp());
            Duration::from_secs(time_until_expiry)
        })
    }
}

#[derive(Default)]
struct OrderMonitorConfig {
    min_deadline: u64,
    peak_prove_khz: Option<u64>,
    max_concurrent_proofs: Option<u32>,
    additional_proof_cycles: u64,
    batch_buffer_time_secs: u64,
    order_commitment_priority: OrderCommitmentPriority,
    priority_addresses: Option<Vec<Address>>,
}

#[derive(Clone)]
pub struct RpcRetryConfig {
    pub retry_count: u64,
    pub retry_sleep_ms: u64,
}

#[derive(Clone)]
pub struct OrderMonitor<P> {
    db: DbObj,
    chain_monitor: Arc<ChainMonitorService<P>>,
    block_time: u64,
    config: ConfigLock,
    market: BoundlessMarketService<Arc<P>>,
    provider: Arc<P>,
    prover_addr: Address,
    priced_order_rx: Arc<Mutex<mpsc::Receiver<Box<OrderRequest>>>>,
    lock_and_prove_cache: Arc<Cache<String, Arc<OrderRequest>>>,
    prove_cache: Arc<Cache<String, Arc<OrderRequest>>>,
    supported_selectors: SupportedSelectors,
    rpc_retry_config: RpcRetryConfig,
}

impl<P> OrderMonitor<P>
where
    P: Provider<Ethereum> + WalletProvider,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db: DbObj,
        provider: Arc<P>,
        chain_monitor: Arc<ChainMonitorService<P>>,
        config: ConfigLock,
        block_time: u64,
        prover_addr: Address,
        market_addr: Address,
        priced_orders_rx: mpsc::Receiver<Box<OrderRequest>>,
        stake_token_decimals: u8,
        rpc_retry_config: RpcRetryConfig,
    ) -> Result<Self> {
        let txn_timeout_opt = {
            let config = config.lock_all().context("Failed to read config")?;
            config.batcher.txn_timeout
        };

        let mut market = BoundlessMarketService::new(
            market_addr,
            provider.clone(),
            provider.default_signer_address(),
        );
        if let Some(txn_timeout) = txn_timeout_opt {
            market = market.with_timeout(Duration::from_secs(txn_timeout));
        }
        {
            let config = config.lock_all()?;

            market = market.with_stake_balance_alert(
                &config
                    .market
                    .stake_balance_warn_threshold
                    .as_ref()
                    .map(|s| parse_units(s, stake_token_decimals).unwrap().into()),
                &config
                    .market
                    .stake_balance_error_threshold
                    .as_ref()
                    .map(|s| parse_units(s, stake_token_decimals).unwrap().into()),
            );
        }
        let monitor = Self {
            db,
            chain_monitor,
            block_time,
            config,
            market,
            provider,
            prover_addr,
            priced_order_rx: Arc::new(Mutex::new(priced_orders_rx)),
            lock_and_prove_cache: Arc::new(Cache::builder().expire_after(OrderExpiry).build()),
            prove_cache: Arc::new(Cache::builder().expire_after(OrderExpiry).build()),
            supported_selectors: SupportedSelectors::default(),
            rpc_retry_config,
        };
        Ok(monitor)
    }

    async fn lock_order(&self, order: &OrderRequest) -> Result<U256, OrderMonitorErr> {
        let request_id = order.request.id;

        let order_status = self
            .market
            .get_status(request_id, Some(order.request.expires_at()))
            .await
            .context("Failed to get request status")?;
        if order_status != RequestStatus::Unknown {
            tracing::info!("Request {:x} not open: {order_status:?}, skipping", request_id);
            // TODO: fetch some chain data to find out who / and for how much the order
            // was locked in at
            return Err(OrderMonitorErr::AlreadyLocked);
        }

        let is_locked = self
            .db
            .is_request_locked(U256::from(order.request.id))
            .await
            .context("Failed to check if request is locked")?;
        if is_locked {
            tracing::warn!("Request 0x{:x} already locked: {order_status:?}, skipping", request_id);
            return Err(OrderMonitorErr::AlreadyLocked);
        }

        let conf_priority_gas = {
            let conf = self.config.lock_all().context("Failed to lock config")?;
            conf.market.lockin_priority_gas
        };

        tracing::info!(
            "Locking request: 0x{:x} for stake: {}",
            request_id,
            order.request.offer.lockStake
        );
        let lock_block = self
            .market
            .lock_request(&order.request, order.client_sig.clone(), conf_priority_gas)
            .await
            .map_err(|e| -> OrderMonitorErr {
                match e {
                    MarketError::TxnError(txn_err) => match txn_err {
                        TxnErr::BoundlessMarketErr(IBoundlessMarketErrors::RequestIsLocked(_)) => {
                            OrderMonitorErr::AlreadyLocked
                        }
                        _ => OrderMonitorErr::LockTxFailed(txn_err.to_string()),
                    },
                    MarketError::RequestAlreadyLocked(_e) => OrderMonitorErr::AlreadyLocked,
                    MarketError::TxnConfirmationError(e) => {
                        OrderMonitorErr::LockTxNotConfirmed(e.to_string())
                    }
                    MarketError::LockRevert(e) => {
                        // Note: lock revert could be for any number of reasons;
                        // 1/ someone may have locked in the block before us,
                        // 2/ the lock may have expired,
                        // 3/ the request may have been fulfilled,
                        // 4/ the requestor may have withdrawn their funds
                        // Currently we don't have a way to determine the cause of the revert.
                        OrderMonitorErr::LockTxFailed(format!("Tx hash 0x{e:x}"))
                    }
                    MarketError::Error(e) => {
                        let error_msg = e.to_string().to_lowercase();
                        
                        // Check for nonce-related errors first
                        if error_msg.contains("nonce too low") || 
                           error_msg.contains("nonce already used") || 
                           error_msg.contains("nonce gap") {
                            tracing::warn!("Lock transaction failed due to nonce error: {e}. This will be retried with fresh nonce.");
                            OrderMonitorErr::LockTxFailed(format!("Nonce error (will retry): {e}"))
                        }
                        // Insufficient balance error is thrown both when the requestor has insufficient balance,
                        // Requestor having insufficient balance can happen and is out of our control. The prover
                        // having insufficient balance is unexpected as we should have checked for that before
                        // committing to locking the order.
                        else if error_msg.contains("insufficientbalance") {
                            let prover_addr_str =
                                self.prover_addr.to_string().to_lowercase().replace("0x", "");
                            if error_msg.contains(&prover_addr_str) {
                                OrderMonitorErr::InsufficientBalance
                            } else {
                                OrderMonitorErr::LockTxFailed(format!(
                                    "Requestor has insufficient balance at lock time: {e}"
                                ))
                            }
                        } else if error_msg.contains("requestislocked") {
                            OrderMonitorErr::AlreadyLocked
                        } else {
                            OrderMonitorErr::UnexpectedError(e)
                        }
                    }
                    _ => {
                        if e.to_string().contains("RequestIsLocked") {
                            OrderMonitorErr::AlreadyLocked
                        } else {
                            OrderMonitorErr::UnexpectedError(e.into())
                        }
                    }
                }
            })?;

        // Fetch the block to retrieve the lock timestamp. This has been observed to return
        // inconsistent state between the receipt being available but the block not yet.
        let lock_timestamp = crate::futures_retry::retry(
            self.rpc_retry_config.retry_count,
            self.rpc_retry_config.retry_sleep_ms,
            || async {
                Ok(self
                    .provider
                    .get_block_by_number(lock_block.into())
                    .await
                    .with_context(|| format!("failed to get block {lock_block}"))?
                    .with_context(|| format!("failed to get block {lock_block}: block not found"))?
                    .header
                    .timestamp)
            },
            "get_block_by_number",
        )
        .await
        .map_err(OrderMonitorErr::UnexpectedError)?;

        let lock_price = order
            .request
            .offer
            .price_at(lock_timestamp)
            .context("Failed to calculate lock price")?;

        Ok(lock_price)
    }

    async fn get_proving_order_capacity(
        &self,
        max_concurrent_proofs: Option<u32>,
        prev_orders_by_status: &mut String,
    ) -> Result<Capacity, OrderMonitorErr> {
        if max_concurrent_proofs.is_none() {
            return Ok(Capacity::Unlimited);
        };

        let max = max_concurrent_proofs.unwrap();
        let committed_orders = self
            .db
            .get_committed_orders()
            .await
            .map_err(|e| OrderMonitorErr::UnexpectedError(e.into()))?;
        let committed_orders_count: u32 = committed_orders.len().try_into().unwrap();

        Self::log_capacity(prev_orders_by_status, committed_orders, max).await;

        let available_slots = max.saturating_sub(committed_orders_count);
        Ok(Capacity::Available(available_slots))
    }

    async fn log_capacity(
        prev_orders_by_status: &mut String,
        commited_orders: Vec<Order>,
        max: u32,
    ) {
        let committed_orders_count: u32 = commited_orders.len().try_into().unwrap();
        let request_id_and_status = commited_orders
            .iter()
            .map(|order| format!("[{:?}]: {order}", order.status))
            .collect::<Vec<_>>();

        let capacity_log = format!("Current num committed orders: {committed_orders_count}. Maximum commitment: {max}. Committed orders: {request_id_and_status:?}");

        // Note: we don't compare previous to capacity_log as it contains timestamps which cause it to always change.
        // We only want to log if status or num orders changes.
        let cur_orders_by_status = commited_orders
            .iter()
            .map(|order| format!("{:?}-{}", order.status, order.id()))
            .collect::<Vec<_>>()
            .join(",");
        if *prev_orders_by_status != cur_orders_by_status {
            tracing::info!("{}", capacity_log);
            *prev_orders_by_status = cur_orders_by_status;
        }
    }

    /// Helper method to skip an order in the database and invalidate the appropriate cache
    async fn skip_order(&self, order: &OrderRequest, reason: &str) {
        if let Err(e) = self.db.insert_skipped_request(order).await {
            tracing::error!("Failed to skip order ({}): {} - {e:?}", reason, order.id());
        }

        match order.fulfillment_type {
            FulfillmentType::LockAndFulfill => {
                self.lock_and_prove_cache.invalidate(&order.id()).await;
            }
            FulfillmentType::FulfillAfterLockExpire | FulfillmentType::FulfillWithoutLocking => {
                self.prove_cache.invalidate(&order.id()).await;
            }
        }
    }

    async fn get_valid_orders(
        &self,
        current_block_timestamp: u64,
        min_deadline: u64,
    ) -> Result<Vec<Arc<OrderRequest>>> {
        let mut candidate_orders: Vec<Arc<OrderRequest>> = Vec::new();

        fn is_within_deadline(
            order: &OrderRequest,
            current_block_timestamp: u64,
            min_deadline: u64,
        ) -> bool {
            if order.request.expires_at() < current_block_timestamp {
                tracing::debug!("Request {:x} has now expired. Skipping.", order.request.id);
                false
            } else if order.request.expires_at().saturating_sub(crate::now_timestamp()) < min_deadline {
                tracing::debug!("Request {:x} deadline at {} is less than the minimum deadline {} seconds required to prove an order. Skipping.", order.request.id, order.request.expires_at(), min_deadline);
                false
            } else {
                true
            }
        }

        fn is_target_time_reached(order: &OrderRequest, current_block_timestamp: u64) -> bool {
            // Note: this could use current timestamp, but avoiding cases where clock has drifted.
            match order.target_timestamp {
                Some(target_timestamp) => {
                    if current_block_timestamp < target_timestamp {
                        tracing::trace!(
                            "Request {:x} target timestamp {} not yet reached (current: {}). Waiting.",
                            order.request.id,
                            target_timestamp,
                            current_block_timestamp
                        );
                        false
                    } else {
                        true
                    }
                }
                None => {
                    // Should not happen, just warning for safety as this condition is not strictly
                    // enforced at compile time.
                    tracing::warn!("Request {:x} has no target timestamp set", order.request.id);
                    false
                }
            }
        }

        for (_, order) in self.prove_cache.iter() {
            let is_fulfilled = self
                .db
                .is_request_fulfilled(U256::from(order.request.id))
                .await
                .context("Failed to check if request is fulfilled")?;
            if is_fulfilled {
                tracing::debug!(
                    "Request 0x{:x} was locked by another prover and was fulfilled. Skipping.",
                    order.request.id
                );
                self.skip_order(&order, "was fulfilled by other").await;
            } else if !is_within_deadline(&order, current_block_timestamp, min_deadline) {
                self.skip_order(&order, "expired").await;
            } else if is_target_time_reached(&order, current_block_timestamp) {
                tracing::info!("Request 0x{:x} was locked by another prover but expired unfulfilled, setting status to pending proving", order.request.id);
                candidate_orders.push(order);
            }
        }

        for (_, order) in self.lock_and_prove_cache.iter() {
            let is_lock_expired = order.request.lock_expires_at() < current_block_timestamp;
            if is_lock_expired {
                tracing::debug!("Request {:x} was scheduled to be locked by us, but its lock has now expired. Skipping.", order.request.id);
                self.skip_order(&order, "lock expired before we locked").await;
            } else if let Some((locker, _)) =
                self.db.get_request_locked(U256::from(order.request.id)).await?
            {
                let our_address = self.provider.default_signer_address().to_string().to_lowercase();
                let locker_address = locker.to_lowercase();
                // Compare normalized addresses (lowercase without 0x prefix)
                let our_address_normalized = our_address.trim_start_matches("0x");
                let locker_address_normalized = locker_address.trim_start_matches("0x");

                if locker_address_normalized != our_address_normalized {
                    tracing::debug!("Request 0x{:x} was scheduled to be locked by us ({}), but is already locked by another prover ({}). Skipping.", order.request.id, our_address, locker_address);
                    self.skip_order(&order, "locked by another prover").await;
                } else {
                    // Edge case where we locked the order, but due to some reason was not moved to proving state. Should not happen.
                    tracing::debug!("Request 0x{:x} was scheduled to be locked by us, but is already locked by us. Proceeding to prove.", order.request.id);
                    candidate_orders.push(order);
                }
            } else if !is_within_deadline(&order, current_block_timestamp, min_deadline) {
                self.skip_order(&order, "insufficient deadline").await;
            } else if is_target_time_reached(&order, current_block_timestamp) {
                candidate_orders.push(order);
            }
        }

        if candidate_orders.is_empty() {
            tracing::trace!(
                "No orders to lock and/or prove as of block timestamp {}",
                current_block_timestamp
            );
            return Ok(Vec::new());
        }

        tracing::debug!(
            "Valid orders that reached target timestamp; ready for locking/proving, num: {}, ids: {}",
            candidate_orders.len(),
            candidate_orders.iter().map(|order| order.id()).collect::<Vec<_>>().join(", ")
        );

        Ok(candidate_orders)
    }

    async fn lock_and_prove_orders(&self, orders: &[Arc<OrderRequest>]) -> Result<()> {
        use futures::stream::{FuturesUnordered, StreamExt};
        
        let mut lock_stream = FuturesUnordered::new();
        
        // Add all lock tasks to the stream
        for order in orders {
            let order_clone = order.clone();
            let self_clone = self.clone();
            
            lock_stream.push(async move {
                let order_id = order_clone.id();
                let start_time = std::time::Instant::now();
                
                if order_clone.fulfillment_type == FulfillmentType::LockAndFulfill {
                    let request_id = order_clone.request.id;
                    match self_clone.lock_order(&order_clone).await {
                        Ok(lock_price) => {
                            let lock_duration = start_time.elapsed();
                            tracing::info!("Locked request: 0x{:x} in {:?}", request_id, lock_duration);
                            
                            if let Err(err) = self_clone.db.insert_accepted_request(&order_clone, lock_price).await {
                                tracing::error!(
                                    "FATAL STAKE AT RISK: {} failed to move from locking -> proving status {}",
                                    order_id,
                                    err
                                );
                            }
                        }
                        Err(ref err) => {
                            match err {
                                OrderMonitorErr::UnexpectedError(inner) => {
                                    tracing::error!(
                                        "Failed to lock order: {order_id} - {} - {inner:?}",
                                        err.code()
                                    );
                                }
                                _ => {
                                    tracing::warn!(
                                        "Soft failed to lock request: {order_id} - {} - {err:?}",
                                        err.code()
                                    );
                                }
                            }
                            if let Err(err) = self_clone.db.insert_skipped_request(&order_clone).await {
                                tracing::error!(
                                    "Failed to set DB failure state for order: {order_id} - {err:?}"
                                );
                            }
                        }
                    }
                    self_clone.lock_and_prove_cache.invalidate(&order_id).await;
                } else {
                    if let Err(err) = self_clone.db.insert_accepted_request(&order_clone, U256::ZERO).await {
                        tracing::error!(
                            "Failed to set order status to pending proving: {} - {err:?}",
                            order_id
                        );
                    }
                    self_clone.prove_cache.invalidate(&order_id).await;
                }
                
                order_id
            });
        }
        
        // Process results as they complete, not waiting for all
        let mut completed_count = 0;
        let total_count = orders.len();
        
        while let Some(order_id) = lock_stream.next().await {
            completed_count += 1;
            tracing::debug!(
                "Completed processing order {} ({}/{})", 
                order_id, 
                completed_count, 
                total_count
            );
        }
        
        tracing::info!(
            "Finished processing {} orders", 
            completed_count
        );

        Ok(())
    }

    /// Calculate the gas units needed for an order and the corresponding cost in wei
    async fn calculate_order_gas_cost_wei(
        &self,
        order: &OrderRequest,
        gas_price: u128,
    ) -> Result<U256, OrderMonitorErr> {
        // Calculate gas units needed for this order (lock + fulfill)
        let order_gas_units = if order.fulfillment_type == FulfillmentType::LockAndFulfill {
            U256::from(utils::estimate_gas_to_lock(&self.config, order).await?).saturating_add(
                U256::from(
                    utils::estimate_gas_to_fulfill(
                        &self.config,
                        &self.supported_selectors,
                        &order.request,
                    )
                    .await?,
                ),
            )
        } else {
            U256::from(
                utils::estimate_gas_to_fulfill(
                    &self.config,
                    &self.supported_selectors,
                    &order.request,
                )
                .await?,
            )
        };

        let order_cost_wei = U256::from(gas_price) * order_gas_units;

        Ok(order_cost_wei)
    }

    async fn apply_capacity_limits(
        &self,
        orders: Vec<Arc<OrderRequest>>,
        config: &OrderMonitorConfig,
        prev_orders_by_status: &mut String,
    ) -> Result<Vec<Arc<OrderRequest>>> {
        let num_orders = orders.len();
        // Get our current capacity for proving orders given our config and the number of orders that are currently committed to be proven + fulfilled.
        let capacity = self
            .get_proving_order_capacity(config.max_concurrent_proofs, prev_orders_by_status)
            .await?;
        let capacity_granted: usize = capacity
            .request_capacity(num_orders.try_into().expect("Failed to convert order count to u32"))
            as usize;

        tracing::info!(
            "Num orders ready for locking and/or proving: {}. Total capacity available: {capacity:?}, Capacity granted: {capacity_granted:?}",
            num_orders
        );

        let mut final_orders: Vec<Arc<OrderRequest>> = Vec::with_capacity(capacity_granted);

        // Get current gas price and available balance
        let gas_price =
            self.chain_monitor.current_gas_price().await.context("Failed to get gas price")?;
        let available_balance_wei = self
            .provider
            .get_balance_flashblocks(self.provider.default_signer_address(), &self.config)
            .await
            .map_err(|err| OrderMonitorErr::RpcErr(err.into()))?;

        // Calculate gas units required for committed orders
        let committed_orders = self.db.get_committed_orders().await?;
        let committed_gas_units =
            futures::future::try_join_all(committed_orders.iter().map(|order| {
                utils::estimate_gas_to_fulfill(
                    &self.config,
                    &self.supported_selectors,
                    &order.request,
                )
            }))
            .await?
            .iter()
            .sum::<u64>();

        // Calculate cost in wei
        let committed_cost_wei = U256::from(gas_price) * U256::from(committed_gas_units);

        // Log committed order gas requirements
        if !committed_orders.is_empty() {
            tracing::debug!(
                "Cost for {} committed orders: {} ether",
                committed_orders.len(),
                format_ether(committed_cost_wei),
            );
        }

        // Ensure we have enough for committed orders
        if committed_cost_wei > available_balance_wei {
            tracing::error!(
                "Insufficient balance for committed orders. Required: {} ether, Available: {} ether",
                format_ether(committed_cost_wei),
                format_ether(available_balance_wei)
            );
            return Ok(Vec::new());
        }

        // Calculate remaining balance after accounting for committed orders
        let mut remaining_balance_wei = available_balance_wei - committed_cost_wei;

        // Apply peak khz limit if specified
        let num_commited_orders = committed_orders.len();
        if config.peak_prove_khz.is_some() && !orders.is_empty() {
            let peak_prove_khz = config.peak_prove_khz.unwrap();
            let total_commited_cycles = committed_orders
                .iter()
                .map(|order| order.total_cycles.unwrap() + config.additional_proof_cycles)
                .sum::<u64>();

            let now = now_timestamp();
            // Estimate the time the prover will be available given our current committed orders.
            let started_proving_at = committed_orders
                .iter()
                .map(|order| order.proving_started_at.unwrap())
                .min()
                .unwrap_or(now);

            let proof_time_seconds = total_commited_cycles.div_ceil(1_000).div_ceil(peak_prove_khz);
            let mut prover_available_at = started_proving_at + proof_time_seconds;
            if prover_available_at < now {
                let seconds_behind = now - prover_available_at;
                tracing::warn!("Proofs are behind what is estimated from peak_prove_khz config by {} seconds. Consider lowering this value to avoid overlocking orders.", seconds_behind);
                prover_available_at = now;
            }
            tracing::debug!("Already committed to {} orders, with a total cycle count of {}, a peak khz limit of {}, started working on them at {}, we estimate the prover will be available in {} seconds", 
                num_commited_orders,
                total_commited_cycles,
                peak_prove_khz,
                started_proving_at,
                prover_available_at.saturating_sub(now),
            );

            // For each order in consideration, check if it can be completed before its expiration
            // and that there is enough gas to pay for the lock and fulfillment of all orders
            // including the committed orders.
            for order in orders {
                if final_orders.len() >= capacity_granted {
                    break;
                }
                // Calculate gas and cost for this order using our helper method
                let order_cost_wei = self.calculate_order_gas_cost_wei(&order, gas_price).await?;

                // Skip if not enough balance
                if order_cost_wei > remaining_balance_wei {
                    tracing::warn!(
                        "Insufficient balance for order {}. Required: {} ether, Remaining: {} ether",
                        order.id(),
                        format_ether(order_cost_wei),
                        format_ether(remaining_balance_wei)
                    );
                    self.skip_order(&order, "insufficient balance").await;
                    continue;
                }

                let Some(order_cycles) = order.total_cycles else {
                    tracing::warn!("Order 0x{:x} has no total cycles, preflight was skipped? Not considering for peak khz limit", order.request.id);
                    final_orders.push(order);
                    remaining_balance_wei -= order_cost_wei;
                    continue;
                };

                // Calculate total cycles including application proof, assessor, and set builder estimates
                let total_cycles = order_cycles + config.additional_proof_cycles;

                let proof_time_seconds = total_cycles.div_ceil(1_000).div_ceil(peak_prove_khz);
                let completion_time = prover_available_at + proof_time_seconds;
                let expiration = match order.fulfillment_type {
                    FulfillmentType::LockAndFulfill => order.request.lock_expires_at(),
                    FulfillmentType::FulfillAfterLockExpire => order.request.expires_at(),
                    _ => panic!("Unsupported fulfillment type: {:?}", order.fulfillment_type),
                };

                if completion_time + config.batch_buffer_time_secs > expiration {
                    // If the order cannot be completed before its expiration, skip it permanently.
                    // Otherwise, we keep the order for the next iteration as capacity may free up in the future.

                    if now + proof_time_seconds > expiration {
                        tracing::info!("Order 0x{:x} cannot be completed before its expiration at {}, proof estimated to take {} seconds and complete at {}. Skipping", 
                            order.request.id,
                            expiration,
                            proof_time_seconds,
                            completion_time
                        );
                        // If the order cannot be completed regardless of other orders, skip it
                        // permanently. Otherwise, will retry including the order.
                        self.skip_order(&order, "cannot be completed before expiration").await;
                    } else {
                        tracing::debug!("Given current commited orders and capacity, order 0x{:x} cannot be completed before its expiration. Not skipping as capacity may free up before it expires.", order.request.id);
                    }
                    continue;
                }

                tracing::debug!("Order {} estimated to take {} seconds (including assessor + set builder), and would be completed at {} ({} seconds from now). It expires at {} ({} seconds from now)", order.id(), proof_time_seconds, completion_time, completion_time.saturating_sub(now_timestamp()), expiration, expiration.saturating_sub(now_timestamp()));

                final_orders.push(order);
                prover_available_at = completion_time;
                remaining_balance_wei -= order_cost_wei;
            }
        } else {
            // If no peak khz limit, just check gas for each order
            for order in orders {
                if final_orders.len() >= capacity_granted {
                    break;
                }
                let order_cost_wei = self.calculate_order_gas_cost_wei(&order, gas_price).await?;

                // Skip if not enough balance
                if order_cost_wei > remaining_balance_wei {
                    tracing::warn!(
                        "Insufficient balance for order {}. Required: {} ether, Remaining: {} ether",
                        order.id(),
                        format_ether(order_cost_wei),
                        format_ether(remaining_balance_wei)
                    );
                    self.skip_order(&order, "insufficient balance").await;
                    continue;
                }

                final_orders.push(order);
                remaining_balance_wei -= order_cost_wei;
            }
        }

        tracing::info!(
            "Started with {} orders ready to be locked and/or proven. Already commited to {} orders. After applying capacity limits of {} max concurrent proofs and {} peak khz, filtered to {} orders: {:?}",
            num_orders,
            num_commited_orders,
            if let Some(max_concurrent_proofs) = config.max_concurrent_proofs {
                max_concurrent_proofs.to_string()
            } else {
                "unlimited".to_string()
            },
            if let Some(peak_prove_khz) = config.peak_prove_khz {
                peak_prove_khz.to_string()
            } else {
                "unlimited".to_string()
            },
            final_orders.len(),
            final_orders.iter().map(|order| order.id()).collect::<Vec<_>>()
        );

        Ok(final_orders)
    }

    pub async fn start_monitor(
        self,
        cancel_token: CancellationToken,
    ) -> Result<(), OrderMonitorErr> {
        let mut last_block = 0;
        let mut first_block = 0;
        let mut interval = tokio::time::interval_at(
            tokio::time::Instant::now(),
            tokio::time::Duration::from_millis(50), // 50ms检查间隔，最大化锁定响应速度
        );
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut new_orders = self.priced_order_rx.lock().await;
        let mut prev_orders_by_status = String::new();

        loop {
            tokio::select! {
                // Process new orders from the channel as soon as they arrive
                biased;

                Some(result) = new_orders.recv() => {
                    // 检查是否是立即锁定的订单
                    let is_immediate = result.target_timestamp == Some(0);

                    if is_immediate {
                        tracing::info!("Processing immediate order {} directly upon receipt", result.id());
                        // 立即处理：先加入缓存，再立即处理
                        self.handle_new_order_result_and_process_immediately(result).await?;
                    } else {
                        // 常规处理：只加入缓存，等待tick
                        self.handle_new_order_result(result).await?;
                    }
                }

                // On each interval, process all pending orders and do the block-based logic
                _ = interval.tick() => {
                    let ChainHead { block_number, block_timestamp } =
                        self.chain_monitor.current_chain_head().await?;
                    if block_number != last_block {
                        last_block = block_number;
                        if first_block == 0 {
                            first_block = block_number;
                        }
                        tracing::trace!(
                            "Order monitor processing block {block_number} at timestamp {block_timestamp}"
                        );

                        let monitor_config = {
                            let config = self.config.lock_all().context("Failed to read config")?;
                            OrderMonitorConfig {
                                min_deadline: config.market.min_deadline,
                                peak_prove_khz: config.market.peak_prove_khz,
                                max_concurrent_proofs: config.market.max_concurrent_proofs,
                                additional_proof_cycles: config.market.additional_proof_cycles,
                                batch_buffer_time_secs: config.batcher.block_deadline_buffer_secs,
                                order_commitment_priority: config.market.order_commitment_priority,
                                priority_addresses: config.market.priority_requestor_addresses.clone(),
                            }
                        };

                        // Get orders that are valid and ready for locking/proving, skipping orders that are now invalid for proving, due to expiring, being locked by another prover, etc.
                        let mut valid_orders = self.get_valid_orders(block_timestamp, monitor_config.min_deadline).await?;

                        if valid_orders.is_empty() {
                            tracing::trace!(
                                "No orders to lock and/or prove as of block timestamp {}",
                                block_timestamp
                            );
                            continue;
                        }

                        // Filter out FulfillAfterLockExpire orders
                        valid_orders.retain(|order| {
                            if order.fulfillment_type == FulfillmentType::FulfillAfterLockExpire {
                                tracing::info!("Skipping FulfillAfterLockExpire order: {}", order.id());
                                false
                            } else {
                                true
                            }
                        });

                        // Prioritize the orders that intend to fulfill based on configured commitment priority.
                        valid_orders = self.prioritize_orders(valid_orders, monitor_config.order_commitment_priority, monitor_config.priority_addresses.as_deref());

                        // Filter down the orders given our max concurrent proofs, peak khz limits, and gas limitations.
                        let final_orders = self
                            .apply_capacity_limits(
                                valid_orders,
                                &monitor_config,
                                &mut prev_orders_by_status,
                            )
                            .await?;

                        tracing::trace!("After processing block {}[timestamp {}], we will now start locking and/or proving {} orders.",
                            block_number,
                            block_timestamp,
                            final_orders.len(),
                        );

                        if !final_orders.is_empty() {
                            // Lock and prove filtered orders.
                            self.lock_and_prove_orders(&final_orders).await?;
                        }
                    }
                }
                _ = cancel_token.cancelled() => {
                    tracing::debug!("Order monitor received cancellation");
                    break;
                }
            }
        }
        Ok(())
    }

    // Called when a new order result is received from the channel
    async fn handle_new_order_result(
        &self,
        order: Box<OrderRequest>,
    ) -> Result<(), OrderMonitorErr> {
        match order.fulfillment_type {
            FulfillmentType::LockAndFulfill => {
                // Note: this could be done without waiting for the batch to minimize latency, but
                //       avoiding more complicated logic for checking capacity for each order.

                // If not, add it to the cache to be locked after target time
                self.lock_and_prove_cache.insert(order.id(), Arc::from(order)).await;
            }
            FulfillmentType::FulfillAfterLockExpire | FulfillmentType::FulfillWithoutLocking => {
                self.prove_cache.insert(order.id(), Arc::from(order)).await;
            }
        }
        Ok(())
    }

    // Handle immediate orders: add to cache and process immediately
    async fn handle_new_order_result_and_process_immediately(
        &self,
        order: Box<OrderRequest>,
    ) -> Result<(), OrderMonitorErr> {
        let order_id = order.id();
        let order_arc: Arc<OrderRequest> = Arc::from(order);

        // 1. 先加入缓存 (与常规流程一致)
        match order_arc.fulfillment_type {
            FulfillmentType::LockAndFulfill => {
                self.lock_and_prove_cache.insert(order_id.clone(), order_arc.clone()).await;
            }
            FulfillmentType::FulfillAfterLockExpire | FulfillmentType::FulfillWithoutLocking => {
                self.prove_cache.insert(order_id.clone(), order_arc.clone()).await;
            }
        }

        // 2. 立即处理订单
        if let Err(e) = self.process_order_immediately_from_cache(order_arc).await {
            tracing::error!("Failed to process immediate order {}: {:?}", order_id, e);
        }

        Ok(())
    }

    // Process a single immediate order from cache directly without waiting for tick
    async fn process_order_immediately_from_cache(
        &self,
        order_arc: Arc<OrderRequest>,
    ) -> Result<(), OrderMonitorErr> {
        let ChainHead { block_number, block_timestamp } =
            self.chain_monitor.current_chain_head().await?;

        let monitor_config = {
            let config = self.config.lock_all().context("Failed to read config")?;
            OrderMonitorConfig {
                min_deadline: config.market.min_deadline,
                peak_prove_khz: config.market.peak_prove_khz,
                max_concurrent_proofs: config.market.max_concurrent_proofs,
                additional_proof_cycles: config.market.additional_proof_cycles,
                batch_buffer_time_secs: config.batcher.block_deadline_buffer_secs,
                order_commitment_priority: config.market.order_commitment_priority,
                priority_addresses: config.market.priority_requestor_addresses.clone(),
            }
        };
        
        // Skip FulfillAfterLockExpire orders
        if order_arc.fulfillment_type == FulfillmentType::FulfillAfterLockExpire {
            tracing::info!("Skipping FulfillAfterLockExpire immediate order: {}", order_arc.id());
            return Ok(());
        }

        // Validate the immediate order
        let is_valid = match order_arc.fulfillment_type {
            FulfillmentType::LockAndFulfill => {
                // Check if lock expired
                let is_lock_expired = order_arc.request.lock_expires_at() < block_timestamp;
                if is_lock_expired {
                    tracing::debug!("Immediate order {} lock expired, skipping", order_arc.id());
                    self.skip_order(&order_arc, "lock expired before we locked").await;
                    return Ok(());
                }

                // Check if already locked by someone else
                if let Some((locker, _)) = self.db.get_request_locked(U256::from(order_arc.request.id)).await.context("Failed to check if request is locked")? {
                    let our_address = self.provider.default_signer_address().to_string().to_lowercase();
                    let locker_address = locker.to_lowercase();
                    let our_address_normalized = our_address.trim_start_matches("0x");
                    let locker_address_normalized = locker_address.trim_start_matches("0x");

                    if locker_address_normalized != our_address_normalized {
                        tracing::debug!("Immediate order {} already locked by another prover, skipping", order_arc.id());
                        self.skip_order(&order_arc, "locked by another prover").await;
                        return Ok(());
                    } else {
                        tracing::debug!("Immediate order {} already locked by us, proceeding to prove", order_arc.id());
                    }
                }

                {
                    // Inline deadline check (same logic as is_within_deadline)
                    if order_arc.request.expires_at() < block_timestamp {
                        tracing::debug!("Request {:x} has now expired. Skipping.", order_arc.request.id);
                        false
                    } else if order_arc.request.expires_at().saturating_sub(crate::now_timestamp()) < monitor_config.min_deadline {
                        tracing::debug!("Request {:x} deadline at {} is less than the minimum deadline {} seconds required to prove an order. Skipping.", order_arc.request.id, order_arc.request.expires_at(), monitor_config.min_deadline);
                        false
                    } else {
                        true
                    }
                }
            }
            _ => {
                // For prove-only orders, check if already fulfilled
                let is_fulfilled = self
                    .db
                    .is_request_fulfilled(U256::from(order_arc.request.id))
                    .await
                    .context("Failed to check if request is fulfilled")?;
                
                if is_fulfilled {
                    tracing::debug!("Immediate order {} was fulfilled by other, skipping", order_arc.id());
                    self.skip_order(&order_arc, "was fulfilled by other").await;
                    return Ok(());
                }

                {
                    // Inline deadline check (same logic as is_within_deadline)
                    if order_arc.request.expires_at() < block_timestamp {
                        tracing::debug!("Request {:x} has now expired. Skipping.", order_arc.request.id);
                        false
                    } else if order_arc.request.expires_at().saturating_sub(crate::now_timestamp()) < monitor_config.min_deadline {
                        tracing::debug!("Request {:x} deadline at {} is less than the minimum deadline {} seconds required to prove an order. Skipping.", order_arc.request.id, order_arc.request.expires_at(), monitor_config.min_deadline);
                        false
                    } else {
                        true
                    }
                }
            }
        };

        if !is_valid {
            self.skip_order(&order_arc, "insufficient deadline").await;
            return Ok(());
        }

        tracing::info!(
            "Processing immediate order {} directly at block {} timestamp {}",
            order_arc.id(),
            block_number,
            block_timestamp
        );

        // Apply capacity limits for immediate order (simplified check)
        let mut prev_orders_by_status = String::new();
        let capacity_checked_orders = self
            .apply_capacity_limits(
                vec![order_arc],
                &monitor_config,
                &mut prev_orders_by_status,
            )
            .await?;

        if capacity_checked_orders.is_empty() {
            tracing::warn!(
                "Immediate order was filtered out by capacity limits, will be processed in next tick cycle"
            );
            return Ok(());
        }

        // Process the immediate order directly
        self.lock_and_prove_orders(&capacity_checked_orders).await?;

        Ok(())
    }

}

impl<P> RetryTask for OrderMonitor<P>
where
    P: Provider<Ethereum> + WalletProvider + 'static + Clone,
{
    type Error = OrderMonitorErr;
    fn spawn(&self, cancel_token: CancellationToken) -> RetryRes<Self::Error> {
        let monitor_clone = self.clone();
        Box::pin(async move {
            tracing::info!("Starting order monitor");
            monitor_clone.start_monitor(cancel_token).await.map_err(SupervisorErr::Recover)?;
            Ok(())
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::OrderStatus;
    use crate::{db::SqliteDb, now_timestamp, FulfillmentType};
    use alloy::node_bindings::AnvilInstance;
    use alloy::{
        network::EthereumWallet,
        node_bindings::Anvil,
        primitives::{Address, U256},
        providers::{
            fillers::{
                BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller,
                WalletFiller,
            },
            ProviderBuilder, RootProvider,
        },
        signers::local::PrivateKeySigner,
    };
    use boundless_market::contracts::{
        Offer, Predicate, PredicateType, ProofRequest, RequestId, RequestInput, RequestInputType,
        Requirements,
    };
    use boundless_market_test_utils::{
        deploy_boundless_market, deploy_hit_points, ASSESSOR_GUEST_ID, ASSESSOR_GUEST_PATH,
    };

    use risc0_zkvm::Digest;
    use std::{future::Future, sync::Arc};
    use tokio::task::JoinSet;
    use tracing_test::traced_test;

    type TestProvider = FillProvider<
        JoinFill<
            JoinFill<
                alloy::providers::Identity,
                JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
            >,
            WalletFiller<EthereumWallet>,
        >,
        RootProvider,
    >;

    pub struct TestCtx {
        pub monitor: OrderMonitor<TestProvider>,
        pub anvil: AnvilInstance,
        pub db: DbObj,
        pub market_address: Address,
        #[allow(dead_code)]
        pub config: ConfigLock,
        pub priced_order_tx: mpsc::Sender<Box<OrderRequest>>,
        pub signer: PrivateKeySigner,
        pub market_service: BoundlessMarketService<Arc<TestProvider>>,
        next_order_id: u32, // Counter to assign unique order IDs
    }

    impl TestCtx {
        // Convert the standalone function to a method on TestCtx
        pub async fn create_test_order(
            &mut self,
            fulfillment_type: FulfillmentType,
            bidding_start: u64,
            lock_timeout: u64,
            timeout: u64,
        ) -> Box<OrderRequest> {
            let request_id = self.next_order_id;
            self.next_order_id += 1;

            let request = ProofRequest::new(
                RequestId::new(self.signer.address(), request_id),
                Requirements::new(
                    Digest::ZERO,
                    Predicate {
                        predicateType: PredicateType::PrefixMatch,
                        data: Default::default(),
                    },
                ),
                "http://risczero.com/image",
                RequestInput { inputType: RequestInputType::Inline, data: Default::default() },
                Offer {
                    minPrice: U256::from(1),
                    maxPrice: U256::from(2),
                    biddingStart: bidding_start,
                    rampUpPeriod: 1,
                    timeout: timeout as u32,
                    lockTimeout: lock_timeout as u32,
                    lockStake: U256::from(0),
                },
            );

            let client_sig = request
                .sign_request(&self.signer, self.market_address, self.anvil.chain_id())
                .await
                .unwrap()
                .as_bytes()
                .into();

            Box::new(OrderRequest {
                target_timestamp: Some(0),
                request,
                image_id: None,
                input_id: None,
                expire_timestamp: None,
                client_sig,
                fulfillment_type,
                boundless_market_address: self.market_address,
                chain_id: self.anvil.chain_id(),
                total_cycles: None,
            })
        }
    }

    pub async fn setup_om_test_context() -> TestCtx {
        let anvil = Anvil::new().spawn();
        let signer: PrivateKeySigner = anvil.keys()[0].clone().into();
        let provider = Arc::new(
            ProviderBuilder::new()
                .wallet(EthereumWallet::from(signer.clone()))
                .connect(&anvil.endpoint())
                .await
                .unwrap(),
        );

        // Deploy contracts
        let hit_points = deploy_hit_points(signer.address(), provider.clone()).await.unwrap();

        let market_address = deploy_boundless_market(
            signer.address(),
            provider.clone(),
            Address::ZERO,
            hit_points,
            Digest::from(ASSESSOR_GUEST_ID),
            format!("file://{ASSESSOR_GUEST_PATH}"),
            Some(signer.address()),
        )
        .await
        .unwrap();

        // Set up market service
        let market_service = BoundlessMarketService::new(
            market_address,
            provider.clone(),
            provider.default_signer_address(),
        );

        // Deposit ETH into the contract for the prover to use when locking orders
        // Using 10 ETH to ensure plenty of funds for tests
        let stake_token_decimals = market_service.stake_token_decimals().await.unwrap();
        market_service
            .deposit(parse_units("10.0", stake_token_decimals).unwrap().into())
            .await
            .unwrap();

        let db: DbObj = Arc::new(SqliteDb::new("sqlite::memory:").await.unwrap());
        let config = ConfigLock::default();

        config.load_write().unwrap().market.min_deadline = 50;
        config.load_write().unwrap().market.lockin_gas_estimate = 200_000;
        config.load_write().unwrap().market.fulfill_gas_estimate = 300_000;
        config.load_write().unwrap().market.groth16_verify_gas_estimate = 50_000;

        let block_time = 2;

        let chain_monitor = Arc::new(ChainMonitorService::new(provider.clone()).await.unwrap());
        tokio::spawn(chain_monitor.spawn(Default::default()));

        // Create required channels for tests
        let (priced_order_tx, priced_order_rx) = mpsc::channel(16);

        let monitor = OrderMonitor::new(
            db.clone(),
            provider.clone(),
            chain_monitor.clone(),
            config.clone(),
            block_time,
            signer.address(),
            market_address,
            priced_order_rx,
            stake_token_decimals,
            RpcRetryConfig { retry_count: 2, retry_sleep_ms: 500 },
        )
        .unwrap();

        TestCtx {
            monitor,
            anvil,
            db,
            market_address,
            config,
            priced_order_tx,
            signer,
            market_service,
            next_order_id: 1, // Initialize with 1 instead of 0
        }
    }

    async fn run_with_monitor<P, F, T>(monitor: OrderMonitor<P>, f: F) -> T
    where
        P: Provider + WalletProvider + Clone + 'static,
        F: Future<Output = T>,
    {
        // A JoinSet automatically aborts all its tasks when dropped
        let mut tasks = JoinSet::new();
        // Spawn the monitor
        tasks.spawn(async move { monitor.start_monitor(Default::default()).await });

        tokio::select! {
            result = f => result,
            monitor_task_result = tasks.join_next() => {
                panic!("Monitor exited unexpectedly: {:?}", monitor_task_result.unwrap());
            },
        }
    }

    #[tokio::test]
    #[traced_test]
    async fn monitor_block() {
        let mut ctx = setup_om_test_context().await;

        // Create a test order using the TestCtx helper method
        let order =
            ctx.create_test_order(FulfillmentType::LockAndFulfill, now_timestamp(), 100, 200).await;

        let order_id = order.id();

        let _request_id =
            ctx.market_service.submit_request(&order.request, &ctx.signer).await.unwrap();

        // Send the order to the monitor
        ctx.priced_order_tx.send(order).await.unwrap();

        run_with_monitor(ctx.monitor, async move {
            // loop for 20 seconds
            for _ in 0..20 {
                let order = ctx.db.get_order(&order_id).await.unwrap();
                if order.is_some() {
                    assert_eq!(order.unwrap().status, OrderStatus::PendingProving);
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }

            let order = ctx.db.get_order(&order_id).await.unwrap().unwrap();
            assert_eq!(order.status, OrderStatus::PendingProving);
        })
        .await;
    }

    // Capacity tests
    #[test]
    fn test_capacity_unlimited() {
        let capacity = Capacity::Unlimited;
        assert_eq!(capacity.request_capacity(0), 0);
        assert_eq!(capacity.request_capacity(15), MAX_PROVING_BATCH_SIZE);
        assert_eq!(capacity.request_capacity(MAX_PROVING_BATCH_SIZE), MAX_PROVING_BATCH_SIZE);
    }

    #[test]
    fn test_capacity_proving() {
        let capacity = Capacity::Available(50);
        assert_eq!(capacity.request_capacity(0), 0);
        assert_eq!(capacity.request_capacity(4), 4);
        assert_eq!(capacity.request_capacity(10), MAX_PROVING_BATCH_SIZE);
    }

    // Filtering tests
    #[tokio::test]
    #[traced_test]
    async fn test_filter_expired_orders() {
        let mut ctx = setup_om_test_context().await;
        let current_timestamp = now_timestamp();

        // Create an expired order
        let expired_order = ctx
            .create_test_order(FulfillmentType::LockAndFulfill, current_timestamp - 100, 50, 50)
            .await;
        let expired_order_id = expired_order.id();
        ctx.monitor
            .lock_and_prove_cache
            .insert(expired_order_id.clone(), Arc::from(expired_order))
            .await;

        let result = ctx.monitor.get_valid_orders(current_timestamp, 0).await.unwrap();

        assert!(result.is_empty());

        let order = ctx.db.get_order(&expired_order_id).await.unwrap().unwrap();
        assert_eq!(order.status, OrderStatus::Skipped);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_filter_insufficient_deadline() {
        let mut ctx = setup_om_test_context().await;
        let current_timestamp = now_timestamp();

        // Create an order with insufficient deadline
        let order =
            ctx.create_test_order(FulfillmentType::LockAndFulfill, current_timestamp, 45, 45).await;
        let order_1_id = order.id();
        ctx.monitor.lock_and_prove_cache.insert(order_1_id.clone(), Arc::from(order)).await;

        // Create an order with insufficient deadline
        let order = ctx
            .create_test_order(FulfillmentType::FulfillAfterLockExpire, current_timestamp, 1, 45)
            .await;
        let order_2_id = order.id();
        ctx.monitor.prove_cache.insert(order_2_id.clone(), Arc::from(order)).await;

        let result = ctx.monitor.get_valid_orders(current_timestamp, 100).await.unwrap();

        assert!(result.is_empty());

        let order = ctx.db.get_order(&order_1_id).await.unwrap().unwrap();
        assert_eq!(order.status, OrderStatus::Skipped);

        let order = ctx.db.get_order(&order_2_id).await.unwrap().unwrap();
        assert_eq!(order.status, OrderStatus::Skipped);
    }

    #[tokio::test]
    async fn test_filter_locked_by_others() {
        let mut ctx = setup_om_test_context().await;
        let current_timestamp = now_timestamp();

        // Create an order that's locked by another prover
        let order = ctx
            .create_test_order(FulfillmentType::LockAndFulfill, current_timestamp, 100, 200)
            .await;
        let order_id = order.id();
        ctx.db
            .set_request_locked(
                U256::from(order.request.id),
                &Address::ZERO.to_string(),
                current_timestamp,
            )
            .await
            .unwrap();
        ctx.monitor.lock_and_prove_cache.insert(order.id(), Arc::from(order)).await;

        let result =
            ctx.monitor.get_valid_orders(current_timestamp, current_timestamp + 100).await.unwrap();

        assert!(result.is_empty());

        let order = ctx.db.get_order(&order_id).await.unwrap().unwrap();
        assert_eq!(order.status, OrderStatus::Skipped);
    }

    // Processing tests
    #[tokio::test]
    #[traced_test]
    async fn test_process_fulfill_after_lock_expire_orders() {
        let mut ctx = setup_om_test_context().await;
        let current_timestamp = now_timestamp();

        let order = ctx
            .create_test_order(FulfillmentType::FulfillAfterLockExpire, current_timestamp, 100, 200)
            .await;
        let order_id = order.id();

        ctx.monitor.lock_and_prove_orders(&[Arc::from(order)]).await.unwrap();

        let updated_order = ctx.db.get_order(&order_id).await.unwrap().unwrap();
        assert_eq!(updated_order.status, OrderStatus::PendingProving);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_apply_capacity_limits_unlimited() {
        let mut ctx = setup_om_test_context().await;
        let current_timestamp = now_timestamp();

        // Create multiple orders
        let mut orders = Vec::new();
        for _ in 1..=5 {
            let order = ctx
                .create_test_order(FulfillmentType::LockAndFulfill, current_timestamp, 100, 200)
                .await;

            // Submit requests to blockchain
            let _request_id =
                ctx.market_service.submit_request(&order.request, &ctx.signer).await.unwrap();

            orders.push(Arc::from(order));
        }

        // Set unlimited capacity in config
        ctx.config.load_write().unwrap().market.max_concurrent_proofs = None;

        // Process all orders with unlimited capacity
        let filtered_orders = ctx
            .monitor
            .apply_capacity_limits(
                orders.clone(),
                &OrderMonitorConfig::default(),
                &mut String::new(),
            )
            .await
            .unwrap();
        let result = ctx.monitor.lock_and_prove_orders(&filtered_orders).await;
        assert!(result.is_ok(), "lock_and_prove_orders should succeed");

        // All orders should be processed since capacity is unlimited
        let mut processed_count = 0;
        for order in orders {
            if let Some(order) = ctx.db.get_order(&order.id()).await.unwrap() {
                processed_count += 1;
                assert_eq!(order.status, OrderStatus::PendingProving);
            }
        }

        // Should process all 5 orders
        assert_eq!(
            processed_count, 5,
            "Should have processed all 5 orders with unlimited capacity"
        );
    }

    #[tokio::test]
    #[traced_test]
    async fn test_apply_capacity_limits_proving() {
        let mut ctx = setup_om_test_context().await;
        let current_timestamp = now_timestamp();

        // Add a committed order to simulate existing workload
        let committed_order = ctx
            .create_test_order(FulfillmentType::LockAndFulfill, current_timestamp, 100, 200)
            .await;
        let mut committed_order = committed_order.to_proving_order(Default::default());
        committed_order.status = OrderStatus::Proving;
        committed_order.proving_started_at = Some(current_timestamp);
        ctx.db.add_order(&committed_order).await.unwrap();

        // Create multiple new orders
        let mut orders = Vec::new();
        for _ in 1..=5 {
            let order = ctx
                .create_test_order(FulfillmentType::LockAndFulfill, current_timestamp, 100, 200)
                .await;

            // Submit requests to blockchain
            let _request_id =
                ctx.market_service.submit_request(&order.request, &ctx.signer).await.unwrap();

            orders.push(Arc::from(order));
        }

        // Process orders with limited capacity
        let filtered_orders = ctx
            .monitor
            .apply_capacity_limits(
                orders,
                &OrderMonitorConfig {
                    max_concurrent_proofs: Some(3),
                    order_commitment_priority: OrderCommitmentPriority::ShortestExpiry,
                    ..Default::default()
                },
                &mut String::new(),
            )
            .await
            .unwrap();
        ctx.monitor.lock_and_prove_orders(&filtered_orders).await.unwrap();

        // Count processed orders
        let mut processed_count = 0;
        for order in filtered_orders {
            if let Some(order) = ctx.db.get_order(&order.id()).await.unwrap() {
                processed_count += 1;
                assert_eq!(order.status, OrderStatus::PendingProving);
            }
        }

        // Should only process 2 more orders (3 total with 1 already committed)
        assert_eq!(
            processed_count, 2,
            "Should have processed only 2 more orders due to concurrent proving capacity limit"
        );
    }

    #[tokio::test]
    #[traced_test]
    async fn test_apply_capacity_limits_committed_work_too_large() {
        let mut ctx = setup_om_test_context().await;
        let current_timestamp = now_timestamp();

        // Add a large committed order to simulate existing workload
        let committed_order = ctx
            .create_test_order(FulfillmentType::LockAndFulfill, current_timestamp, 100, 200)
            .await;
        let mut committed_order = committed_order.to_proving_order(Default::default());
        committed_order.status = OrderStatus::Proving;
        committed_order.total_cycles = Some(10_000_000_000_000_000);
        committed_order.proving_started_at = Some(current_timestamp);
        ctx.db.add_order(&committed_order).await.unwrap();

        let mut orders = Vec::new();

        let mut order1 = ctx
            .create_test_order(FulfillmentType::LockAndFulfill, current_timestamp, 100, 200)
            .await;
        order1.total_cycles = Some(1000);

        orders.push(Arc::from(order1));

        let mut order2 = ctx
            .create_test_order(FulfillmentType::LockAndFulfill, current_timestamp, 100, 200)
            .await;
        order2.total_cycles = Some(100);
        orders.push(Arc::from(order2));

        let filtered_orders = ctx
            .monitor
            .apply_capacity_limits(
                orders,
                &OrderMonitorConfig { peak_prove_khz: Some(100), ..Default::default() },
                &mut String::new(),
            )
            .await
            .unwrap();

        assert_eq!(filtered_orders.len(), 0);
        assert!(logs_contain("cannot be completed before its expiration"));
        assert!(logs_contain("Started with 2 orders"));
        assert!(logs_contain("filtered to 0 orders: []"));
    }

    #[tokio::test]
    async fn test_apply_capacity_limits_skip_proof_time_past_expiration() {
        let mut ctx = setup_om_test_context().await;
        let current_timestamp = now_timestamp();

        // Create orders with different expiration times
        let mut candidate_orders = Vec::new();

        // Order 1: Will expire soon (not enough time to prove)
        let mut order1 =
            ctx.create_test_order(FulfillmentType::LockAndFulfill, current_timestamp, 5, 5).await;
        order1.total_cycles = Some(1000000000000);
        let order1_id = order1.id();

        let _request_id =
            ctx.market_service.submit_request(&order1.request, &ctx.signer).await.unwrap();
        candidate_orders.push(Arc::from(order1));

        // Order 2: Longer expiration (enough time to prove)
        let mut order2 = ctx
            .create_test_order(FulfillmentType::LockAndFulfill, current_timestamp, 100, 200)
            .await;
        order2.total_cycles = Some(2000);
        let order2_id = order2.id();
        let _request_id =
            ctx.market_service.submit_request(&order2.request, &ctx.signer).await.unwrap();
        candidate_orders.push(Arc::from(order2));

        let filtered_orders = ctx
            .monitor
            .apply_capacity_limits(
                candidate_orders,
                &OrderMonitorConfig { peak_prove_khz: Some(1), ..Default::default() },
                &mut String::new(),
            )
            .await
            .unwrap();

        assert_eq!(filtered_orders[0].total_cycles, Some(2000));
        assert_eq!(filtered_orders[0].id(), order2_id);

        // The first order should be skipped due to insufficient proof time before expiration
        let order1_db = ctx.db.get_order(&order1_id).await.unwrap();
        assert_eq!(
            order1_db.unwrap().status,
            OrderStatus::Skipped,
            "Order 1 should be skipped due to insufficient time to complete proof"
        );
    }

    #[tokio::test]
    async fn test_gas_estimation_functions() {
        let mut ctx = setup_om_test_context().await;

        // Create orders with different fulfillment types to test gas estimation for each type
        let lock_and_fulfill_order =
            ctx.create_test_order(FulfillmentType::LockAndFulfill, now_timestamp(), 100, 200).await;
        let lock_and_fulfill_id = lock_and_fulfill_order.id();

        let fulfill_only_order = ctx
            .create_test_order(FulfillmentType::FulfillAfterLockExpire, now_timestamp(), 100, 200)
            .await;
        let fulfill_only_id = fulfill_only_order.id();

        let _lock_request_id = ctx
            .market_service
            .submit_request(&lock_and_fulfill_order.request, &ctx.signer)
            .await
            .unwrap();
        let _fulfill_request_id = ctx
            .market_service
            .submit_request(&fulfill_only_order.request, &ctx.signer)
            .await
            .unwrap();

        let orders = vec![Arc::from(lock_and_fulfill_order), Arc::from(fulfill_only_order)];
        let filtered_orders = ctx
            .monitor
            .apply_capacity_limits(orders, &OrderMonitorConfig::default(), &mut String::new())
            .await
            .unwrap();
        let result = ctx.monitor.lock_and_prove_orders(&filtered_orders).await;
        assert!(result.is_ok(), "lock_and_prove_orders should succeed");

        // Verify both orders were processed correctly
        let lock_order_result = ctx.db.get_order(&lock_and_fulfill_id).await.unwrap();
        let fulfill_order_result = ctx.db.get_order(&fulfill_only_id).await.unwrap();

        assert!(lock_order_result.is_some(), "Lock and fulfill order should be processed");
        assert!(fulfill_order_result.is_some(), "Fulfill only order should be processed");

        assert_eq!(lock_order_result.unwrap().status, OrderStatus::PendingProving);
        assert_eq!(fulfill_order_result.unwrap().status, OrderStatus::PendingProving);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_multiple_orders_khz_capacity() {
        let mut ctx = setup_om_test_context().await;
        ctx.config.load_write().unwrap().market.max_concurrent_proofs = None;

        // Create multiple orders with increasing cycle counts to test gas allocation
        let mut orders = Vec::new();
        for i in 1..6 {
            let mut order = ctx
                .create_test_order(FulfillmentType::LockAndFulfill, now_timestamp(), 120, 120)
                .await;

            // Set increasing cycle counts to test different gas requirements
            order.total_cycles = Some(i as u64 * 1_000_000);

            let _request_id =
                ctx.market_service.submit_request(&order.request, &ctx.signer).await.unwrap();

            orders.push(Arc::from(order));
        }

        let filtered_orders = ctx
            .monitor
            .apply_capacity_limits(
                orders,
                &OrderMonitorConfig { peak_prove_khz: Some(100), ..Default::default() },
                &mut String::new(),
            )
            .await
            .unwrap();

        println!("filtered_orders: {filtered_orders:?}");
        // 100khz can prove 1m+2m+3m+4m (10m) cycles in 100 seconds
        assert_eq!(filtered_orders.len(), 4);

        assert_eq!(filtered_orders[0].total_cycles, Some(1_000_000));
        assert_eq!(filtered_orders[3].total_cycles, Some(4_000_000));
    }

    #[tokio::test]
    #[traced_test]
    async fn test_insufficient_balance_committed_orders() {
        let mut ctx = setup_om_test_context().await;

        let balance = ctx.monitor.provider.get_balance(ctx.signer.address()).await.unwrap();
        let gas_price = ctx.monitor.provider.get_gas_price().await.unwrap();
        let gas_remaining: u64 = (balance / U256::from(gas_price)).try_into().unwrap();
        ctx.config.load_write().unwrap().market.fulfill_gas_estimate = gas_remaining / 2;
        ctx.config.load_write().unwrap().market.lockin_gas_estimate = gas_remaining / 3;

        let incoming_order =
            ctx.create_test_order(FulfillmentType::LockAndFulfill, now_timestamp(), 100, 200).await;

        let mut orders = vec![Arc::from(incoming_order)];

        // Should be able to have enough gas for 1 lock and fulfill
        let filtered_orders = ctx
            .monitor
            .apply_capacity_limits(
                orders.clone(),
                &OrderMonitorConfig::default(),
                &mut String::new(),
            )
            .await
            .unwrap();
        assert_eq!(filtered_orders.len(), 1);

        orders.push(Arc::from(
            ctx.create_test_order(FulfillmentType::LockAndFulfill, now_timestamp(), 100, 200).await,
        ));

        // Should still only be able to have enough gas for 1 lock and fulfill
        let filtered_orders = ctx
            .monitor
            .apply_capacity_limits(
                orders.clone(),
                &OrderMonitorConfig::default(),
                &mut String::new(),
            )
            .await
            .unwrap();
        assert_eq!(filtered_orders.len(), 1);

        for _ in 0..3 {
            let committed_order = ctx
                .create_test_order(FulfillmentType::LockAndFulfill, now_timestamp(), 100, 200)
                .await;

            let mut committed_order_obj = committed_order.to_proving_order(Default::default());
            committed_order_obj.status = OrderStatus::Proving;
            committed_order_obj.proving_started_at = Some(now_timestamp());
            ctx.db.add_order(&committed_order_obj).await.unwrap();
        }

        // Process the order - with insufficient balance for committed orders
        let filtered_orders = ctx
            .monitor
            .apply_capacity_limits(orders, &OrderMonitorConfig::default(), &mut String::new())
            .await
            .unwrap();

        assert!(filtered_orders.is_empty());
    }

    #[tokio::test]
    #[traced_test]
    async fn test_target_timestamp_prevents_early_locking() {
        let mut ctx = setup_om_test_context().await;
        let current_timestamp = now_timestamp();
        let future_timestamp = current_timestamp + 100; // 100 seconds in the future

        // Create orders of both types and set them to be picked up at a future timestamp.
        let mut lock_and_fulfill_order = ctx
            .create_test_order(FulfillmentType::LockAndFulfill, current_timestamp, 200, 300)
            .await;

        lock_and_fulfill_order.target_timestamp = Some(future_timestamp);
        let lock_and_fulfill_order_id = lock_and_fulfill_order.id();

        ctx.monitor
            .lock_and_prove_cache
            .insert(lock_and_fulfill_order.id(), Arc::from(lock_and_fulfill_order))
            .await;

        let mut fulfill_after_expire_order = ctx
            .create_test_order(
                FulfillmentType::FulfillAfterLockExpire,
                current_timestamp - 50,
                10,
                300,
            )
            .await;

        fulfill_after_expire_order.target_timestamp = Some(future_timestamp);
        let fulfill_after_expire_order_id = fulfill_after_expire_order.id();

        // Simulate that this order was locked by another prover but the lock has now expired
        ctx.db
            .set_request_locked(
                U256::from(fulfill_after_expire_order.request.id),
                &Address::ZERO.to_string(),
                current_timestamp - 50,
            )
            .await
            .unwrap();

        ctx.monitor
            .prove_cache
            .insert(fulfill_after_expire_order.id(), Arc::from(fulfill_after_expire_order))
            .await;

        // Call get_valid_orders with current timestamp - this should NOT return either order
        // because their target_timestamp is in the future
        let valid_orders = ctx.monitor.get_valid_orders(current_timestamp, 50).await.unwrap();

        assert!(
            valid_orders.is_empty(),
            "Orders with future target_timestamp should not be valid yet, got {} orders",
            valid_orders.len()
        );

        // Verify both orders are still in their respective caches and not skipped
        let cached_lock_order =
            ctx.monitor.lock_and_prove_cache.get(&lock_and_fulfill_order_id).await;
        assert!(cached_lock_order.is_some(), "LockAndFulfill order should still be in cache");

        let cached_prove_order = ctx.monitor.prove_cache.get(&fulfill_after_expire_order_id).await;
        assert!(
            cached_prove_order.is_some(),
            "FulfillAfterLockExpire order should still be in cache"
        );

        // Now test with future timestamp - both orders should be valid
        let valid_orders_in_future =
            ctx.monitor.get_valid_orders(future_timestamp + 1, 50).await.unwrap();

        assert_eq!(
            valid_orders_in_future.len(),
            2,
            "Both orders should be valid when current time >= target_timestamp"
        );

        assert!(valid_orders_in_future.iter().any(|order| order.id() == lock_and_fulfill_order_id));
        assert!(valid_orders_in_future
            .iter()
            .any(|order| order.id() == fulfill_after_expire_order_id));
    }
}

