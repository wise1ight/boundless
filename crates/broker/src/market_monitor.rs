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

use std::sync::Arc;

use alloy::{
    network::Ethereum,
    primitives::{Address, U256},
    providers::Provider,
    rpc::types::Filter,
    sol,
    sol_types::SolEvent,
};

use anyhow::{Context, Result};
use boundless_market::{
    contracts::{
        boundless_market::BoundlessMarketService, IBoundlessMarket, RequestId, RequestStatus,
    },
    order_stream_client::OrderStreamClient,
};
use futures_util::StreamExt;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    chain_monitor::ChainMonitorService,
    db::{DbError, DbObj},
    errors::{impl_coded_debug, CodedError},
    task::{RetryRes, RetryTask, SupervisorErr},
    FulfillmentType, OrderRequest, OrderStateChange,
};
use thiserror::Error;

const BLOCK_TIME_SAMPLE_SIZE: u64 = 10;

#[derive(Error)]
pub enum MarketMonitorErr {
    #[error("{code} Event polling failed: {0:?}", code = self.code())]
    EventPollingErr(anyhow::Error),

    #[error("{code} Log processing failed: {0:?}", code = self.code())]
    LogProcessingFailed(anyhow::Error),

    #[error("{code} Unexpected error: {0:?}", code = self.code())]
    UnexpectedErr(#[from] anyhow::Error),

    #[error("{code} Receiver dropped", code = self.code())]
    ReceiverDropped,
}

impl CodedError for MarketMonitorErr {
    fn code(&self) -> &str {
        match self {
            MarketMonitorErr::EventPollingErr(_) => "[B-MM-501]",
            MarketMonitorErr::LogProcessingFailed(_) => "[B-MM-502]",
            MarketMonitorErr::UnexpectedErr(_) => "[B-MM-500]",
            MarketMonitorErr::ReceiverDropped => "[B-MM-502]",
        }
    }
}

impl_coded_debug!(MarketMonitorErr);

pub struct MarketMonitor<P> {
    lookback_blocks: u64,
    market_addr: Address,
    provider: Arc<P>,
    db: DbObj,
    chain_monitor: Arc<ChainMonitorService<P>>,
    prover_addr: Address,
    order_stream: Option<OrderStreamClient>,
    new_order_tx: mpsc::Sender<Box<OrderRequest>>,
    order_state_tx: broadcast::Sender<OrderStateChange>,
}

sol! {
    #[sol(rpc)]
    interface IERC1271 {
        function isValidSignature(bytes32 hash, bytes memory signature) external view returns (bytes4 magicValue);
    }
}

const ERC1271_MAGIC_VALUE: [u8; 4] = [0x16, 0x26, 0xba, 0x7e];

impl<P> MarketMonitor<P>
where
    P: Provider<Ethereum> + 'static + Clone,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        lookback_blocks: u64,
        market_addr: Address,
        provider: Arc<P>,
        db: DbObj,
        chain_monitor: Arc<ChainMonitorService<P>>,
        prover_addr: Address,
        order_stream: Option<OrderStreamClient>,
        new_order_tx: mpsc::Sender<Box<OrderRequest>>,
        order_state_tx: broadcast::Sender<OrderStateChange>,
    ) -> Self {
        Self {
            lookback_blocks,
            market_addr,
            provider,
            db,
            chain_monitor,
            prover_addr,
            order_stream,
            new_order_tx,
            order_state_tx,
        }
    }

    /// Queries chain history to sample for the median block time
    pub async fn get_block_time(&self) -> Result<u64> {
        let current_block = self.chain_monitor.current_block_number().await?;

        let mut timestamps = vec![];
        let sample_start = current_block - std::cmp::min(current_block, BLOCK_TIME_SAMPLE_SIZE);
        for i in sample_start..current_block {
            let block = self
                .provider
                .get_block_by_number(i.into())
                .await
                .with_context(|| format!("Failed get block {i}"))?
                .with_context(|| format!("Missing block {i}"))?;

            timestamps.push(block.header.timestamp);
        }

        let mut block_times =
            timestamps.windows(2).map(|elm| elm[1] - elm[0]).collect::<Vec<u64>>();
        block_times.sort();

        Ok(block_times[block_times.len() / 2])
    }

    async fn find_open_orders(
        lookback_blocks: u64,
        market_addr: Address,
        provider: Arc<P>,
        chain_monitor: Arc<ChainMonitorService<P>>,
        new_order_tx: &mpsc::Sender<Box<OrderRequest>>,
    ) -> Result<u64, MarketMonitorErr> {
        let current_block = chain_monitor.current_block_number().await?;
        let chain_id = provider.get_chain_id().await.context("Failed to get chain id")?;

        let start_block = current_block.saturating_sub(lookback_blocks);

        tracing::info!("Searching for existing open orders: {start_block} - {current_block}");

        let market = BoundlessMarketService::new(market_addr, provider.clone(), Address::ZERO);
        // let event: Event<_, _, IBoundlessMarket::RequestSubmitted, _> = Event::new(
        //     provider.clone(),
        //     Filter::new().from_block(start_block).address(market_addr),
        // );

        // let logs = event.query().await.context("Failed to query RequestSubmitted events")?;

        let filter = Filter::new()
            .event_signature(IBoundlessMarket::RequestSubmitted::SIGNATURE_HASH)
            .from_block(start_block)
            .address(market_addr);

        // TODO: This could probably be cleaned up but the alloy examples
        // don't have a lot of clean log decoding samples, and the Event::query()
        // interface would randomly fail for me?
        let logs = provider.get_logs(&filter).await.context("Failed to get logs")?;
        let decoded_logs = logs.iter().filter_map(|log| {
            match log.log_decode::<IBoundlessMarket::RequestSubmitted>() {
                Ok(res) => Some(res),
                Err(err) => {
                    tracing::error!("Failed to decode RequestSubmitted log: {err:?}");
                    None
                }
            }
        });

        tracing::debug!("Found {} possible in the past {} blocks", logs.len(), lookback_blocks);
        let mut order_count = 0;
        for log in decoded_logs {
            let event = &log.inner.data;
            let request_id = U256::from(event.requestId);

            let req_status =
                match market.get_status(request_id, Some(event.request.expires_at())).await {
                    Ok(val) => val,
                    Err(err) => {
                        tracing::warn!("Failed to get request status: {err:?}");
                        continue;
                    }
                };

            if !matches!(req_status, RequestStatus::Unknown) {
                tracing::debug!(
                    "Skipping order {request_id:x} reason: order status no longer bidding: {req_status:?}",
                );
                continue;
            }

            let fulfillment_type = match req_status {
                RequestStatus::Locked => FulfillmentType::FulfillAfterLockExpire,
                _ => FulfillmentType::LockAndFulfill,
            };

            tracing::info!(
                "Found open order: {request_id:x} with request status: {req_status:?}, preparing to process with fulfillment type: {fulfillment_type:?}",
            );

            let new_order = OrderRequest::new(
                event.request.clone(),
                event.clientSignature.clone(),
                fulfillment_type,
                market_addr,
                chain_id,
            );

            new_order_tx
                .send(Box::new(new_order))
                .await
                .map_err(|_| MarketMonitorErr::ReceiverDropped)?;
            order_count += 1;
        }

        tracing::info!("Found {order_count} open orders");

        Ok(order_count)
    }

    async fn monitor_orders(
        market_addr: Address,
        provider: Arc<P>,
        new_order_tx: mpsc::Sender<Box<OrderRequest>>,
        cancel_token: CancellationToken,
    ) -> Result<(), MarketMonitorErr> {
        let chain_id = provider.get_chain_id().await.context("Failed to get chain id")?;

        let market = BoundlessMarketService::new(market_addr, provider.clone(), Address::ZERO);
        // TODO: RPC providers can drop filters over time or flush them
        // we should try and move this to a subscription filter if we have issue with the RPC
        // dropping filters

        let event = market
            .instance()
            .RequestSubmitted_filter()
            .watch()
            .await
            .context("Failed to subscribe to RequestSubmitted event")?;
        tracing::info!("Subscribed to RequestSubmitted event");

        let mut stream = event.into_stream();
        loop {
            tokio::select! {
                log_res = stream.next() => {
                    match log_res {
                        Some(Ok((event, _))) => {
                            if let Err(err) = Self::process_event(
                                event,
                                provider.clone(),
                                market_addr,
                                chain_id,
                                &new_order_tx,
                            )
                            .await
                            {
                                let event_err = MarketMonitorErr::LogProcessingFailed(err);
                                tracing::error!("Failed to process event log: {event_err:?}");
                            }
                        }
                        Some(Err(err)) => {
                            let event_err = MarketMonitorErr::EventPollingErr(anyhow::anyhow!(err));
                            tracing::warn!("Failed to fetch event log: {event_err:?}");
                        }
                        None => {
                            return Err(MarketMonitorErr::EventPollingErr(anyhow::anyhow!(
                                "Event polling exited, polling failed (possible RPC error)"
                            )));
                        }
                    }
                }
                _ = cancel_token.cancelled() => {
                    return Ok(());
                }
            }
        }
    }

    /// Monitors the RequestLocked events and updates the database accordingly.
    #[allow(clippy::too_many_arguments)]
    async fn monitor_order_locks(
        market_addr: Address,
        prover_addr: Address,
        provider: Arc<P>,
        db: DbObj,
        new_order_tx: mpsc::Sender<Box<OrderRequest>>,
        order_stream: Option<OrderStreamClient>,
        order_state_tx: broadcast::Sender<OrderStateChange>,
        cancel_token: CancellationToken,
    ) -> Result<(), MarketMonitorErr> {
        let market = BoundlessMarketService::new(market_addr, provider.clone(), Address::ZERO);
        let chain_id = provider.get_chain_id().await.context("Failed to get chain id")?;
        let event = market
            .instance()
            .RequestLocked_filter()
            .watch()
            .await
            .context("Failed to subscribe to RequestLocked event")?;
        tracing::info!("Subscribed to RequestLocked event");

        let mut stream = event.into_stream();
        loop {
            tokio::select! {
                log_res = stream.next() => {
                    match log_res {
                        Some(Ok((event, log))) => {
                            tracing::debug!(
                                "Detected request 0x{:x} locked by {:x}",
                                event.requestId,
                                event.prover,
                            );
                            if let Err(e) = db
                                .set_request_locked(
                                    U256::from(event.requestId),
                                    &event.prover.to_string(),
                                    log.block_number.unwrap(),
                                )
                                .await
                            {
                                match e {
                                    DbError::SqlUniqueViolation(_) => {
                                        tracing::warn!("Duplicate request locked detected {:x}: {e:?}", event.requestId);
                                    }
                                    _ => {
                                        tracing::error!("Failed to store request locked for request {:x} in db: {e:?}", event.requestId);
                                    }
                                }
                            }

                            // Send order state change message for any active preflight of this order
                            let state_change = OrderStateChange::Locked {
                                request_id: U256::from(event.requestId),
                                prover: event.prover,
                            };
                            if let Err(e) = order_state_tx.send(state_change) {
                                tracing::warn!("Failed to send order state change message for request {:x}: {e:?}", event.requestId);
                            }

                            // If the request was not locked by the prover, we create an order to evaluate the request
                            // for fulfilling after the lock expires.
                            if event.prover != prover_addr {
                                // Try to get from market first. If the request was submitted via the order stream, we will be unable to find it there.
                                // In that case we check the order stream.
                                let mut order: Option<OrderRequest> = None;
                                if let Ok((proof_request, signature)) = market.get_submitted_request(event.requestId, None).await {
                                    order = Some(OrderRequest::new(
                                        proof_request,
                                        signature,
                                        FulfillmentType::FulfillAfterLockExpire,
                                        market_addr,
                                        chain_id,
                                    ));
                                } else if let Some(order_stream) = &order_stream {
                                    if let Ok(order_stream_order) = order_stream.fetch_order(event.requestId, None).await {
                                        let proof_request = order_stream_order.request;
                                        let signature = order_stream_order.signature;
                                        order = Some(OrderRequest::new(
                                            proof_request,
                                            signature.as_bytes().into(),
                                            FulfillmentType::FulfillAfterLockExpire,
                                            market_addr,
                                            chain_id,
                                        ));
                                    }
                                }

                                if let Some(order) = order {
                                    if let Err(e) = new_order_tx.send(Box::new(order)).await {
                                        tracing::error!("Failed to send order locked by another prover, {:x}: {e:?}", event.requestId);
                                    }
                                } else {
                                    tracing::warn!("Failed to get order from market or order stream for locked request {:x}. Unable to evaluate for fulfillment after lock expires.", event.requestId);
                                }
                            }
                        }
                        Some(Err(err)) => {
                            let event_err = MarketMonitorErr::EventPollingErr(anyhow::anyhow!(err));
                            tracing::warn!("Failed to fetch RequestLocked event log: {event_err:?}");
                        }
                        None => {
                            return Err(MarketMonitorErr::EventPollingErr(anyhow::anyhow!(
                                "Event polling exited, polling failed (possible RPC error)",
                            )));
                        }
                    }
                }
                _ = cancel_token.cancelled() => {
                    return Ok(());
                }
            }
        }
    }

    /// Monitors the RequestFulfilled events and updates the database accordingly.
    async fn monitor_order_fulfillments(
        market_addr: Address,
        provider: Arc<P>,
        db: DbObj,
        order_state_tx: broadcast::Sender<OrderStateChange>,
        cancel_token: CancellationToken,
    ) -> Result<(), MarketMonitorErr> {
        let market = BoundlessMarketService::new(market_addr, provider.clone(), Address::ZERO);
        let event = market
            .instance()
            .RequestFulfilled_filter()
            .watch()
            .await
            .context("Failed to subscribe to RequestFulfilled event")?;
        tracing::info!("Subscribed to RequestFulfilled event");

        let mut stream = event.into_stream();
        loop {
            tokio::select! {
                log_res = stream.next() => {
                    match log_res {
                        Some(Ok((event, log))) => {
                            tracing::debug!("Detected request fulfilled 0x{:x}", event.requestId);
                            if let Err(e) = db
                                .set_request_fulfilled(
                                    U256::from(event.requestId),
                                    log.block_number.unwrap(),
                                )
                                .await
                            {
                                match e {
                                    DbError::SqlUniqueViolation(_) => {
                                        tracing::warn!("Duplicate fulfillment event detected: {e:?}");
                                    }
                                    _ => {
                                        tracing::error!(
                                            "Failed to store fulfillment for request id {:x}: {e:?}",
                                            event.requestId
                                        );
                                    }
                                }
                            }

                            // Send order state change message
                            let state_change = OrderStateChange::Fulfilled {
                                request_id: U256::from(event.requestId),
                            };
                            if let Err(e) = order_state_tx.send(state_change) {
                                tracing::warn!("Failed to send order state change message for fulfilled request {:x}: {e:?}", event.requestId);
                            }
                        }
                        Some(Err(err)) => {
                            let event_err = MarketMonitorErr::EventPollingErr(anyhow::anyhow!(err));
                            tracing::warn!("Failed to fetch RequestFulfilled event log: {event_err:?}");
                        }
                        None => {
                            return Err(MarketMonitorErr::EventPollingErr(anyhow::anyhow!(
                                "Event polling order fulfillments exited, polling failed (possible RPC error)",
                            )));
                        }
                    }
                }
                _ = cancel_token.cancelled() => {
                    return Ok(());
                }
            }
        }
    }

    async fn process_event(
        event: IBoundlessMarket::RequestSubmitted,
        provider: Arc<P>,
        market_addr: Address,
        chain_id: u64,
        new_order_tx: &mpsc::Sender<Box<OrderRequest>>,
    ) -> Result<()> {
        tracing::info!("Detected new on-chain request 0x{:x}", event.requestId);
        // Check the request id flag to determine if the request is smart contract signed. If so we verify the
        // ERC1271 signature by calling isValidSignature on the smart contract client. Otherwise we verify the
        // the signature as an ECDSA signature.
        let request_id = RequestId::from_lossy(event.requestId);
        if request_id.smart_contract_signed {
            let erc1271 = IERC1271::new(request_id.addr, provider);
            let request_hash = event.request.signing_hash(market_addr, chain_id)?;
            tracing::debug!(
                "Validating ERC1271 signature for request 0x{:x}, calling contract: {} with hash {:x}",
                event.requestId,
                request_id.addr,
                request_hash
            );
            match erc1271.isValidSignature(request_hash, event.clientSignature.clone()).call().await
            {
                Ok(magic_value) => {
                    if magic_value != ERC1271_MAGIC_VALUE {
                        tracing::warn!("Invalid ERC1271 signature for request 0x{:x}, contract: {} returned magic value: 0x{:x}", event.requestId, request_id.addr, magic_value);
                        return Ok(());
                    }
                }
                Err(err) => {
                    tracing::warn!("Failed to call ERC1271 isValidSignature for request 0x{:x}, contract: {} - {err:?}", event.requestId, request_id.addr);
                    return Ok(());
                }
            }
        } else if let Err(err) =
            event.request.verify_signature(&event.clientSignature, market_addr, chain_id)
        {
            tracing::warn!("Failed to validate order signature: 0x{:x} - {err:?}", event.requestId);
            return Ok(()); // Return early without propagating the error if signature verification fails.
        }

        let new_order = OrderRequest::new(
            event.request.clone(),
            event.clientSignature.clone(),
            FulfillmentType::LockAndFulfill,
            market_addr,
            chain_id,
        );

        let order_id = new_order.id();
        if let Err(e) = new_order_tx.send(Box::new(new_order)).await {
            tracing::error!("Failed to send new on-chain order {} to OrderPicker: {}", order_id, e);
        } else {
            tracing::trace!("Sent new on-chain order {} to OrderPicker via channel.", order_id);
        }
        Ok(())
    }
}

impl<P> RetryTask for MarketMonitor<P>
where
    P: Provider<Ethereum> + 'static + Clone,
{
    type Error = MarketMonitorErr;
    fn spawn(&self, cancel_token: CancellationToken) -> RetryRes<Self::Error> {
        let lookback_blocks = self.lookback_blocks;
        let market_addr = self.market_addr;
        let provider = self.provider.clone();
        let prover_addr = self.prover_addr;
        let chain_monitor = self.chain_monitor.clone();
        let new_order_tx = self.new_order_tx.clone();
        let db = self.db.clone();
        let order_stream = self.order_stream.clone();
        let order_state_tx = self.order_state_tx.clone();

        Box::pin(async move {
            tracing::info!("Starting up market monitor");

            Self::find_open_orders(
                lookback_blocks,
                market_addr,
                provider.clone(),
                chain_monitor,
                &new_order_tx,
            )
            .await
            .map_err(|err| {
                tracing::error!("Monitor failed to find open orders on startup.");
                SupervisorErr::Recover(err)
            })?;

            tokio::try_join!(
                Self::monitor_orders(
                    market_addr,
                    provider.clone(),
                    new_order_tx.clone(),
                    cancel_token.clone()
                ),
                Self::monitor_order_fulfillments(
                    market_addr,
                    provider.clone(),
                    db.clone(),
                    order_state_tx.clone(),
                    cancel_token.clone()
                ),
                Self::monitor_order_locks(
                    market_addr,
                    prover_addr,
                    provider.clone(),
                    db,
                    new_order_tx,
                    order_stream,
                    order_state_tx,
                    cancel_token
                )
            )
            .map_err(SupervisorErr::Recover)?;

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db::SqliteDb, now_timestamp};
    use alloy::{
        network::EthereumWallet,
        node_bindings::Anvil,
        primitives::{Address, U256},
        providers::{ext::AnvilApi, ProviderBuilder, WalletProvider},
        signers::local::PrivateKeySigner,
        sol_types::eip712_domain,
    };
    use boundless_market::{
        contracts::{
            boundless_market::{BoundlessMarketService, FulfillmentTx},
            hit_points::default_allowance,
            AssessorReceipt, Offer, Predicate, PredicateType, ProofRequest, RequestInput,
            RequestInputType, Requirements,
        },
        input::GuestEnv,
    };
    use boundless_market_test_utils::{
        create_test_ctx, deploy_boundless_market, mock_singleton, TestCtx, ASSESSOR_GUEST_ID,
        ASSESSOR_GUEST_PATH, ECHO_ID,
    };
    use risc0_zkvm::sha::Digest;

    #[tokio::test]
    async fn find_orders() {
        let anvil = Anvil::new().spawn();
        let signer: PrivateKeySigner = anvil.keys()[0].clone().into();
        let provider = Arc::new(
            ProviderBuilder::new()
                .wallet(EthereumWallet::from(signer.clone()))
                .connect(&anvil.endpoint())
                .await
                .unwrap(),
        );

        let market_address = deploy_boundless_market(
            signer.address(),
            provider.clone(),
            Address::ZERO,
            Address::ZERO,
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

        let min_price = 1;
        let max_price = 10;
        let proving_request = ProofRequest {
            id: boundless_market.request_id_from_nonce().await.unwrap(),
            requirements: Requirements::new(
                Digest::ZERO,
                Predicate { predicateType: PredicateType::PrefixMatch, data: Default::default() },
            ),
            imageUrl: "test".to_string(),
            input: RequestInput { inputType: RequestInputType::Url, data: Default::default() },
            offer: Offer {
                minPrice: U256::from(min_price),
                maxPrice: U256::from(max_price),
                biddingStart: now_timestamp() - 5,
                timeout: 1000,
                lockTimeout: 1000,
                rampUpPeriod: 1,
                lockStake: U256::from(0),
            },
        };

        boundless_market.submit_request(&proving_request, &signer).await.unwrap();

        // let event: Event<_, _, IBoundlessMarket::RequestSubmitted, _> = Event::new(&provider,
        // Filter::new());

        // tx_receipt.inner.logs().into_iter().map(|log| Ok((decode_log(&log)?, log))).collect()

        let chain_monitor = Arc::new(ChainMonitorService::new(provider.clone()).await.unwrap());
        tokio::spawn(chain_monitor.spawn(Default::default()));

        let (order_tx, mut order_rx) = mpsc::channel(16);
        let orders =
            MarketMonitor::find_open_orders(2, market_address, provider, chain_monitor, &order_tx)
                .await
                .unwrap();
        assert_eq!(orders, 1);

        order_rx.try_recv().unwrap();
        assert!(order_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn block_times() {
        let anvil = Anvil::new().spawn();
        let signer: PrivateKeySigner = anvil.keys()[0].clone().into();
        let provider = Arc::new(
            ProviderBuilder::new()
                .wallet(EthereumWallet::from(signer))
                .connect(&anvil.endpoint())
                .await
                .unwrap(),
        );

        provider.anvil_mine(Some(10), Some(2)).await.unwrap();

        let chain_monitor = Arc::new(ChainMonitorService::new(provider.clone()).await.unwrap());
        tokio::spawn(chain_monitor.spawn(Default::default()));
        let (order_tx, _order_rx) = mpsc::channel(16);
        let db: DbObj = Arc::new(SqliteDb::new("sqlite::memory:").await.unwrap());
        let (order_state_tx, _) = broadcast::channel(16);
        let market_monitor = MarketMonitor::new(
            1,
            Address::ZERO,
            provider,
            db,
            chain_monitor,
            Address::ZERO,
            None,
            order_tx,
            order_state_tx,
        );

        let block_time = market_monitor.get_block_time().await.unwrap();
        assert_eq!(block_time, 2);
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_e2e_monitor() {
        // Setup anvil
        let anvil = Anvil::new().spawn();

        let ctx = create_test_ctx(&anvil).await.unwrap();

        let eip712_domain = eip712_domain! {
            name: "IBoundlessMarket",
            version: "1",
            chain_id: anvil.chain_id(),
            verifying_contract: *ctx.customer_market.instance().address(),
        };

        let request = new_request(1, &ctx).await;
        let expires_at = request.expires_at();

        let request_id =
            ctx.customer_market.submit_request(&request, &ctx.customer_signer).await.unwrap();

        // fetch logs to retrieve the customer signature from the event
        let logs = ctx.customer_market.instance().RequestSubmitted_filter().query().await.unwrap();

        let (event, _) = logs.first().unwrap();
        let request = &event.request;
        let customer_sig = event.clientSignature.clone();

        // Deposit prover balances
        let deposit = default_allowance();
        ctx.prover_market.deposit_stake_with_permit(deposit, &ctx.prover_signer).await.unwrap();

        // Lock the request
        ctx.prover_market.lock_request(request, customer_sig, None).await.unwrap();
        assert!(ctx.customer_market.is_locked(request_id).await.unwrap());
        assert!(
            ctx.customer_market.get_status(request_id, Some(expires_at)).await.unwrap()
                == RequestStatus::Locked
        );

        // mock the fulfillment
        let (root, set_verifier_seal, fulfillment, assessor_seal) =
            mock_singleton(request, eip712_domain, ctx.prover_signer.address());

        // publish the committed root
        ctx.set_verifier.submit_merkle_root(root, set_verifier_seal).await.unwrap();

        let assessor_fill = AssessorReceipt {
            seal: assessor_seal,
            selectors: vec![],
            prover: ctx.prover_signer.address(),
            callbacks: vec![],
        };
        // fulfill the request
        ctx.prover_market
            .fulfill(FulfillmentTx::new(vec![fulfillment.clone()], assessor_fill.clone()))
            .await
            .unwrap();
        assert!(ctx.customer_market.is_fulfilled(request_id).await.unwrap());

        // retrieve journal and seal from the fulfilled request
        let (journal, seal) =
            ctx.customer_market.get_request_fulfillment(request_id).await.unwrap();

        assert_eq!(journal, fulfillment.journal);
        assert_eq!(seal, fulfillment.seal);
    }

    async fn new_request<P: Provider>(idx: u32, ctx: &TestCtx<P>) -> ProofRequest {
        ProofRequest::new(
            RequestId::new(ctx.customer_signer.address(), idx),
            Requirements::new(
                Digest::from(ECHO_ID),
                Predicate { predicateType: PredicateType::PrefixMatch, data: Default::default() },
            ),
            "http://image_uri.null",
            GuestEnv::builder().build_inline().unwrap(),
            Offer {
                minPrice: U256::from(20000000000000u64),
                maxPrice: U256::from(40000000000000u64),
                biddingStart: now_timestamp(),
                timeout: 100,
                rampUpPeriod: 1,
                lockStake: U256::from(10),
                lockTimeout: 100,
            },
        )
    }
}
