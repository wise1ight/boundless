// Copyright (c) 2025 RISC Zero, Inc.
//
// All rights reserved.

//! Mempool Monitor for detecting new orders from pending transactions
//! 
//! This module provides functionality to monitor the mempool (pending transactions)
//! and extract new order submissions before they are confirmed on-chain.
//! This allows for faster order detection and locking.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use alloy::{
    consensus::Transaction as TransactionTrait,
    network::Ethereum,
    primitives::{Address, TxHash},
    providers::Provider,
    rpc::types::Transaction,
    sol_types::SolCall,
};
use anyhow::{anyhow, Context, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};
use thiserror::Error;

use boundless_market::contracts::IBoundlessMarket;

use crate::{
    config::{ConfigLock, MempoolConf},
    errors::CodedError,
    flashblocks_utils::is_flashblocks_enabled,
    impl_coded_debug,
    task::{RetryRes, RetryTask, SupervisorErr},
    FulfillmentType, OrderRequest,
};

#[derive(Error)]
pub enum MempoolMonitorErr {
    #[error("{code} RPC error: {0:?}", code = self.code())]
    RpcErr(anyhow::Error),

    #[error("{code} receiver dropped", code = self.code())]
    ReceiverDropped,

    #[error("{code} transaction decode error: {0}", code = self.code())]
    DecodeErr(String),

    #[error("{code} unexpected error: {0:?}", code = self.code())]
    UnexpectedErr(anyhow::Error),
}

impl_coded_debug!(MempoolMonitorErr);

impl From<anyhow::Error> for MempoolMonitorErr {
    fn from(err: anyhow::Error) -> Self {
        Self::UnexpectedErr(err)
    }
}

impl CodedError for MempoolMonitorErr {
    fn code(&self) -> &str {
        match self {
            Self::RpcErr(_) => "[B-MP-001]",
            Self::ReceiverDropped => "[B-MP-002]", 
            Self::DecodeErr(_) => "[B-MP-003]",
            Self::UnexpectedErr(_) => "[B-MP-500]",
        }
    }
}

/// Configuration for mempool monitoring
#[derive(Debug, Clone)]
pub struct MempoolConfig {
    /// Whether mempool monitoring is enabled
    pub enabled: bool,
    /// Polling interval for pending transactions in milliseconds
    pub poll_interval_ms: u64,
    /// Maximum number of pending transactions to process per poll
    pub max_pending_tx_per_poll: usize,
    /// Timeout for individual RPC calls in seconds
    pub rpc_timeout_secs: u64,
    /// Whether to use pending nonce monitoring for faster detection
    pub use_nonce_monitoring: bool,
    /// Addresses to monitor for nonce changes (for faster detection)
    pub monitored_addresses: Vec<Address>,
}

impl From<&MempoolConf> for MempoolConfig {
    fn from(conf: &MempoolConf) -> Self {
        let monitored_addresses = conf.monitored_addresses
            .iter()
            .filter_map(|addr_str| addr_str.parse::<Address>().ok())
            .collect();

        Self {
            enabled: conf.enabled,
            poll_interval_ms: conf.poll_interval_ms,
            max_pending_tx_per_poll: conf.max_pending_tx_per_poll,
            rpc_timeout_secs: conf.rpc_timeout_secs,
            use_nonce_monitoring: conf.use_nonce_monitoring,
            monitored_addresses,
        }
    }
}

impl Default for MempoolConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            poll_interval_ms: 1000,
            max_pending_tx_per_poll: 100,
            rpc_timeout_secs: 5,
            use_nonce_monitoring: true,
            monitored_addresses: vec![],
        }
    }
}

/// Monitors the mempool for new order submissions
#[derive(Clone)]
pub struct MempoolMonitor<P> {
    provider: Arc<P>,
    config: ConfigLock,
    market_addr: Address,
    chain_id: u64,
    new_order_tx: mpsc::Sender<Box<OrderRequest>>,
    mempool_config: MempoolConfig,
    processed_txs: Arc<tokio::sync::Mutex<HashSet<TxHash>>>,
}

impl<P> MempoolMonitor<P>
where
    P: Provider<Ethereum> + Clone + 'static,
{
    pub fn new(
        provider: Arc<P>,
        config: ConfigLock,
        market_addr: Address,
        chain_id: u64,
        new_order_tx: mpsc::Sender<Box<OrderRequest>>,
        mempool_config: MempoolConfig,
    ) -> Self {
        Self {
            provider,
            config,
            market_addr,
            chain_id,
            new_order_tx,
            mempool_config,
            processed_txs: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
        }
    }

    /// Start monitoring the mempool
    async fn monitor(&self, cancel_token: CancellationToken) -> Result<(), MempoolMonitorErr> {
        if !self.mempool_config.enabled {
            info!("Mempool monitoring is disabled");
            return Ok(());
        }

        if !is_flashblocks_enabled(&self.config) {
            warn!("Mempool monitoring works best with Flashblocks enabled");
        }

        info!("Starting mempool monitor for market {:x}", self.market_addr);
        
        // Check if RPC supports mempool queries
        let mempool_supported = self.check_mempool_support().await;
        if !mempool_supported {
            warn!("RPC does not fully support mempool queries, relying on nonce monitoring only");
        }
        
        let mut poll_interval = interval(Duration::from_millis(self.mempool_config.poll_interval_ms));
        let mut nonce_interval = interval(Duration::from_millis(500)); // More frequent nonce checks
        let mut cleanup_interval = interval(Duration::from_secs(60)); // Cleanup every 1 minute

        let mut last_nonces: std::collections::HashMap<Address, u64> = std::collections::HashMap::new();
        let mut consecutive_errors = 0;
        const MAX_CONSECUTIVE_ERRORS: u32 = 10;

        loop {
            tokio::select! {
                _ = poll_interval.tick(), if mempool_supported && consecutive_errors < MAX_CONSECUTIVE_ERRORS => {
                    match self.poll_pending_transactions().await {
                        Ok(_) => {
                            consecutive_errors = 0; // Reset error counter on success
                        }
                        Err(e) => {
                            consecutive_errors += 1;
                            let should_retry = self.handle_rpc_error(&e, "poll_pending_transactions").await;
                            if !should_retry || consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                                warn!("Disabling pending transaction polling due to persistent errors");
                                // Continue with nonce monitoring only
                            } else {
                                sleep(Duration::from_secs(2)).await;
                            }
                        }
                    }
                }
                _ = nonce_interval.tick(), if self.mempool_config.use_nonce_monitoring => {
                    if let Err(e) = self.check_nonce_changes(&mut last_nonces).await {
                        debug!("Error checking nonce changes: {:?}", e);
                    }
                }
                _ = cleanup_interval.tick() => {
                    self.cleanup_processed_txs().await;
                }
                _ = cancel_token.cancelled() => {
                    info!("Mempool monitor received cancellation");
                    break;
                }
            }
        }
        
        Ok(())
    }

    /// Poll pending transactions and extract order submissions
    async fn poll_pending_transactions(&self) -> Result<(), anyhow::Error> {
        trace!("Polling pending transactions");

        // Try to get pending transactions - this might not be supported on all networks
        let pending_txs = match self.get_pending_transactions().await {
            Ok(txs) => txs,
            Err(e) => {
                debug!("Failed to get pending transactions: {}. Falling back to nonce monitoring.", e);
                return Ok(()); // Continue with nonce monitoring
            }
        };

        trace!("Found {} pending transactions", pending_txs.len());

        let max_process = self.mempool_config.max_pending_tx_per_poll;
        let txs_to_process: Vec<_> = pending_txs.into_iter().take(max_process).collect();
        
        // Process transactions in parallel using FuturesUnordered
        let mut futures = FuturesUnordered::new();
        
        for tx in txs_to_process {
            let self_clone = self.clone();
            futures.push(async move {
                self_clone.process_pending_transaction(tx).await
            });
        }

        let mut processed_count = 0;
        let mut error_count = 0;
        
        // Process results as they complete
        while let Some(result) = futures.next().await {
            match result {
                Ok(_) => processed_count += 1,
                Err(e) => {
                    error_count += 1;
                    debug!("Error processing pending transaction: {:?}", e);
                }
            }
        }

        if processed_count > 0 || error_count > 0 {
            debug!("Processed {} pending transactions ({} errors)", processed_count, error_count);
        }

        Ok(())
    }

    /// Get pending transactions from the mempool
    async fn get_pending_transactions(&self) -> Result<Vec<Transaction>, anyhow::Error> {
        // Try different methods to get pending transactions
        
        // Method 1: Try eth_pendingTransactions (if supported)
        if let Ok(txs) = self.try_pending_transactions_rpc().await {
            return Ok(txs);
        }

        // Method 2: Try pending block method
        if let Ok(txs) = self.try_pending_block_method().await {
            return Ok(txs);
        }

        // Method 3: Use nonce monitoring as fallback
        self.try_nonce_based_detection().await
    }

    /// Try to get pending transactions via eth_pendingTransactions RPC
    async fn try_pending_transactions_rpc(&self) -> Result<Vec<Transaction>, anyhow::Error> {
        // This is a custom RPC call that may not be supported on all networks
        let client = self.provider.root().client();
        
        // Add timeout to prevent hanging on slow RPC nodes
        let timeout_duration = Duration::from_secs(self.mempool_config.rpc_timeout_secs);
        let result = tokio::time::timeout(timeout_duration, async {
            let result: Result<Vec<Transaction>, _> = client
                .request("eth_pendingTransactions", ())
                .await
                .map_err(|e| anyhow!("eth_pendingTransactions not supported: {}", e));
            result
        })
        .await
        .map_err(|_| anyhow!("eth_pendingTransactions timed out after {} seconds", self.mempool_config.rpc_timeout_secs))??;
        
        Ok(result)
    }

    /// Try to get transactions from pending block
    async fn try_pending_block_method(&self) -> Result<Vec<Transaction>, anyhow::Error> {
        let timeout_duration = Duration::from_secs(self.mempool_config.rpc_timeout_secs);
        
        let pending_block = tokio::time::timeout(
            timeout_duration,
            self.provider.get_block_by_number(alloy::eips::BlockNumberOrTag::Pending)
        )
        .await
        .map_err(|_| anyhow!("Timeout getting pending block"))?
        .context("Failed to get pending block")?;

        let Some(block) = pending_block else {
            return Ok(vec![]);
        };

        // Get transaction hashes from the block and fetch full transactions
        let tx_hashes = match &block.transactions {
            alloy::rpc::types::BlockTransactions::Full(txs) => {
                return Ok(txs.clone());
            }
            alloy::rpc::types::BlockTransactions::Hashes(hashes) => hashes.clone(),
            alloy::rpc::types::BlockTransactions::Uncle => {
                return Ok(vec![]);
            }
        };

        // Fetch full transaction details in parallel
        let mut futures = FuturesUnordered::new();
        for hash in tx_hashes.into_iter().take(self.mempool_config.max_pending_tx_per_poll) {
            let provider = self.provider.clone();
            futures.push(async move {
                provider.get_transaction_by_hash(hash).await
            });
        }

        let mut transactions = Vec::new();
        while let Some(result) = futures.next().await {
            if let Ok(Some(tx)) = result {
                transactions.push(tx);
            }
        }

        Ok(transactions)
    }

    /// Use nonce monitoring to detect new transactions
    async fn try_nonce_based_detection(&self) -> Result<Vec<Transaction>, anyhow::Error> {
        // This is a fallback method - we return empty vec and rely on nonce monitoring
        Ok(vec![])
    }

    /// Check for nonce changes to detect new transactions
    async fn check_nonce_changes(&self, last_nonces: &mut std::collections::HashMap<Address, u64>) -> Result<(), anyhow::Error> {
        // Check nonces in parallel for faster detection
        let mut futures = FuturesUnordered::new();
        
        for address in &self.mempool_config.monitored_addresses {
            let provider = self.provider.clone();
            let addr = *address;
            futures.push(async move {
                let nonce = provider
                    .get_transaction_count(addr)
                    .pending()
                    .await
                    .context("Failed to get pending nonce")?;
                Ok::<_, anyhow::Error>((addr, nonce))
            });
        }

        // Collect results
        while let Some(result) = futures.next().await {
            match result {
                Ok((address, current_nonce)) => {
                    let last_nonce = last_nonces.get(&address).copied().unwrap_or(0);
                    
                    if current_nonce > last_nonce {
                        debug!("Detected nonce increase for {:x}: {} -> {}", address, last_nonce, current_nonce);
                        // Could trigger more targeted pending transaction fetching here
                        last_nonces.insert(address, current_nonce);
                    }
                }
                Err(e) => {
                    debug!("Failed to check nonce: {:?}", e);
                }
            }
        }

        Ok(())
    }

    /// Process a single pending transaction
    async fn process_pending_transaction(&self, tx: Transaction) -> Result<(), MempoolMonitorErr> {
        let tx_hash = *tx.inner.hash();
        
        // Check if we've already processed this transaction
        {
            let mut processed = self.processed_txs.lock().await;
            if processed.contains(&tx_hash) {
                return Ok(());
            }
            processed.insert(tx_hash);
        }

        // Check if transaction is to our market contract  
        if tx.to() != Some(self.market_addr) {
            return Ok(());
        }

        trace!("Processing pending transaction {:x} to market contract", tx_hash);

        // Try to decode the transaction input
        match self.decode_order_submission(&tx).await {
            Ok(order_request) => {
                info!("Detected new order submission in mempool: {}", order_request.id());
                
                // Send the order to the processing pipeline
                self.new_order_tx
                    .send(Box::new(order_request))
                    .await
                    .map_err(|_| MempoolMonitorErr::ReceiverDropped)?;
            }
            Err(MempoolMonitorErr::DecodeErr(_)) => {
                // Not a submitRequest transaction, ignore
            }
            Err(e) => return Err(e),
        }

        Ok(())
    }

    /// Decode a transaction to extract order submission
    async fn decode_order_submission(&self, tx: &Transaction) -> Result<OrderRequest, MempoolMonitorErr> {
        let input = tx.input();
        
        if input.len() < 4 {
            return Err(MempoolMonitorErr::DecodeErr("Transaction input too short".to_string()));
        }
        
        // Try to decode as submitRequest function call using IBoundlessMarket ABI
        match IBoundlessMarket::submitRequestCall::abi_decode(input) {
            Ok(call_data) => {
                trace!("Successfully decoded submitRequest call for request {}", call_data.request.id);
                
                let order = OrderRequest::new(
                    call_data.request,
                    call_data.clientSignature,
                    FulfillmentType::LockAndFulfill, // Default to lock and fulfill for mempool orders
                    self.market_addr,
                    self.chain_id,
                );

                Ok(order)
            }
            Err(e) => {
                trace!("Failed to decode as submitRequest: {}", e);
                Err(MempoolMonitorErr::DecodeErr(format!("Not a submitRequest call: {}", e)))
            }
        }
    }

    /// Clean up old processed transaction hashes to prevent memory leak
    async fn cleanup_processed_txs(&self) {
        let mut processed = self.processed_txs.lock().await;
        
        // Keep only the last 500 transaction hashes (更频繁清理，保持更小缓存)
        if processed.len() > 500 {
            let excess = processed.len() - 250; // Keep 250, remove the rest
            let to_remove: Vec<TxHash> = processed.iter().take(excess).cloned().collect();
            for hash in to_remove {
                processed.remove(&hash);
            }
            debug!("Cleaned up {} old transaction hashes from mempool monitor cache", excess);
        }
    }

    /// Check if the RPC supports mempool queries
    async fn check_mempool_support(&self) -> bool {
        // Try a simple pending transaction count query
        match self.provider.get_transaction_count(Address::ZERO).pending().await {
            Ok(_) => {
                debug!("RPC supports pending queries");
                true
            }
            Err(e) => {
                warn!("RPC may not fully support pending queries: {}", e);
                false
            }
        }
    }

    /// Handle RPC errors with appropriate retry logic
    async fn handle_rpc_error(&self, error: &anyhow::Error, operation: &str) -> bool {
        if error.to_string().contains("Method not found") ||
           error.to_string().contains("not supported") {
            warn!("RPC method {} not supported, disabling mempool monitoring", operation);
            false // Don't retry if method is not supported
        } else if error.to_string().contains("timeout") ||
                 error.to_string().contains("connection") {
            debug!("RPC {} failed with temporary error: {}, will retry", operation, error);
            true // Retry on temporary errors
        } else {
            warn!("RPC {} failed with unknown error: {}, will retry", operation, error);
            true // Retry on unknown errors by default
        }
    }
}



impl<P> RetryTask for MempoolMonitor<P>
where
    P: Provider<Ethereum> + Clone + 'static,
{
    type Error = MempoolMonitorErr;

    fn spawn(&self, cancel_token: CancellationToken) -> RetryRes<Self::Error> {
        let monitor = Self {
            provider: self.provider.clone(),
            config: self.config.clone(),
            market_addr: self.market_addr,
            chain_id: self.chain_id,
            new_order_tx: self.new_order_tx.clone(),
            mempool_config: self.mempool_config.clone(),
            processed_txs: self.processed_txs.clone(),
        };
        
        Box::pin(async move {
            monitor.monitor(cancel_token).await.map_err(SupervisorErr::Recover)?;
            Ok(())
        })
    }
} 
