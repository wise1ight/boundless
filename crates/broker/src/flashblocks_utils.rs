// Copyright (c) 2025 RISC Zero, Inc.
//
// All rights reserved.

//! Flashblocks utilities for the broker.
//! 
//! This module provides helper functions and extensions to properly use Flashblocks
//! when enabled, falling back to normal operation when disabled.

use alloy::{
    eips::BlockNumberOrTag,
    network::Ethereum,
    primitives::{Address, U256},
    providers::Provider,
};
use anyhow::Result;

use crate::config::ConfigLock;

/// Extension trait to add Flashblocks-aware methods to providers
pub trait FlashblocksProviderExt<P>
where
    P: Provider<Ethereum>,
{
    /// Get balance using the appropriate block tag (pending for Flashblocks, latest otherwise)
    async fn get_balance_flashblocks(&self, address: Address, config: &ConfigLock) -> Result<U256, alloy::transports::TransportError>;
    
    /// Get transaction count using the appropriate block tag (pending for Flashblocks, latest otherwise)  
    async fn get_transaction_count_flashblocks(&self, address: Address, config: &ConfigLock) -> Result<u64, alloy::transports::TransportError>;
}

impl<P> FlashblocksProviderExt<P> for P
where
    P: Provider<Ethereum>,
{
    async fn get_balance_flashblocks(&self, address: Address, config: &ConfigLock) -> Result<U256, alloy::transports::TransportError> {
        let block_tag = get_flashblocks_block_tag(config);
        match block_tag {
            BlockNumberOrTag::Pending => self.get_balance(address).pending().await,
            _ => self.get_balance(address).await,
        }
    }
    
    async fn get_transaction_count_flashblocks(&self, address: Address, config: &ConfigLock) -> Result<u64, alloy::transports::TransportError> {
        let block_tag = get_flashblocks_block_tag(config);
        match block_tag {
            BlockNumberOrTag::Pending => self.get_transaction_count(address).pending().await,
            _ => self.get_transaction_count(address).latest().await,
        }
    }
}

/// Determine which block tag to use based on Flashblocks configuration
pub fn get_flashblocks_block_tag(config: &ConfigLock) -> BlockNumberOrTag {
    let config = config.lock_all().unwrap();
    if config.network.enable_flashblocks.unwrap_or(false) {
        // Use 'pending' for Flashblocks to get the latest Flashblock (200ms confirmation)
        BlockNumberOrTag::Pending
    } else {
        // Use 'latest' for normal operation  
        BlockNumberOrTag::Latest
    }
}

/// Check if Flashblocks is enabled in the configuration
pub fn is_flashblocks_enabled(config: &ConfigLock) -> bool {
    let config = config.lock_all().unwrap();
    config.network.enable_flashblocks.unwrap_or(false)
} 