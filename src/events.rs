use bcs::from_bytes;
use log::warn;
use serde::{Deserialize, Serialize};
use sui_types::base_types::{ObjectID, SuiAddress};
use sui_types::full_checkpoint_content::CheckpointData;
use sui_types::id::ID;
use crate::events::ScallopEvent::BorrowFlashloanEvent;

#[derive(Serialize,Deserialize,Debug)]
enum ScallopEvent {
    BorrowEvent(BorrowEvent),
    BorrowEventV2(BorrowEventV2),
    BorrowFlashloanEvent(BorrowFlashLoanEvent),
    CollateralDepositEvent(CollateralDepositEvent),
    CollateralWithdrawEvent(CollateralWithdrawEvent),
    LiquidateEvent(LiquidateEvent),
    MintEvent(MintEvent),
    ObligationCreatedEvent(ObligationCreatedEvent),
    ObligationLocked(ObligationLocked),
    ObligationUnlocked(ObligationUnlocked),
    RedeemEvent(RedeemEvent),
    RepayEvent(RepayEvent),
    RepayFlashLoanEvent(RepayFlashLoanEvent),
}


// in this moment not sure if the events have constant size ...
pub fn parse(bytes: &[u8], type_: &str) -> Option<ScallopEvent> {
    let result = type_.split("::").last().unwrap();
    match result {
        "BorrowEvent" => {
            let event = from_bytes::<BorrowEvent>(&bytes).unwrap();
            return Some(ScallopEvent::BorrowEvent(event));
        }
        "BorrowFlashLoanEvent" => {
            let event = from_bytes::<BorrowFlashloanEvent>(&bytes).unwrap();
            return Some(ScallopEvent::BorrowFlashloanEvent(event));
        }
        "BorrowEventV2" => {
            let event = from_bytes::<BorrowEventV2>(&bytes).unwrap();
            return Some(ScallopEvent::BorrowEventV2(event));
        }
        "CollateralDepositEvent" => {
            let event = from_bytes::<CollateralDepositEvent>(&bytes).unwrap();
            return Some(ScallopEvent::CollateralDepositEvent(event));
        }
        "CollateralWithdrawEvent" => {
            let event = from_bytes::<CollateralWithdrawEvent>(&bytes).unwrap();
            return Some(ScallopEvent::CollateralWithdrawEvent(event));
        }
        "LiquidateEvent" => {
            let event = from_bytes::<LiquidateEvent>(&bytes).unwrap();
            return Some(ScallopEvent::LiquidateEvent(event));
        }
        "MintEvent" => {
            let event = from_bytes::<MintEvent>(&bytes).unwrap();
            return Some(ScallopEvent::MintEvent(event));
        }
        "ObligationCreatedEvent" => {
            let event = from_bytes::<ObligationCreatedEvent>(&bytes).unwrap();
            return Some(ScallopEvent::ObligationCreatedEvent(event));
        }
        "ObligationLocked" => {
            let event = from_bytes::<ObligationLocked>(&bytes).unwrap();
            return Some(ScallopEvent::ObligationLocked(event));
        }
        "ObligationUnlocked" => {
            let event = from_bytes::<ObligationUnlocked>(&bytes).unwrap();
            return Some(ScallopEvent::ObligationUnlocked(event));
        }
        "RedeemEvent" => {
            let event = from_bytes::<RedeemEvent>(&bytes).unwrap();
            return Some(ScallopEvent::RedeemEvent(event));
        }
        "RepayEvent" => {
            let event = from_bytes::<RepayEvent>(&bytes).unwrap();
            return Some(ScallopEvent::RepayEvent(event));
        }
        "RepayFlashLoanEvent" => {
            let event = from_bytes::<RepayFlashLoanEvent>(&bytes).unwrap();
            return Some(ScallopEvent::RepayFlashLoanEvent(event));
        }
        _ => {
            warn!("pattern for parsing event not found ...")
        }
    }
    return None;
}

#[derive(Serialize,Deserialize,Debug)]
pub struct IndexerData {
    pub digest: String,
    pub checkpoint: u64,
    pub epoch: u64,
    pub data: Vec<u8>,
    pub index: u64,
    pub type_: String,
}

impl IndexerData {
    pub fn parse_event(&self) -> Option<ScallopEvent> {
        return parse(&self.data, self.type_);
    }
}

pub fn process_txn(data: &CheckpointData, filter: &Vec<ObjectID>) -> Vec<(String, IndexerData)>{
    let mut results = vec![];
    for txn in data.transactions.iter() {
        for events in txn.events.iter() {
            for (idx, event) in events.data.iter().enumerate() {
                if filter.contains(&event.package_id) {
                    let digest = txn.transaction.digest().to_string();
                    let result = IndexerData{
                        digest: digest.clone(),
                        checkpoint: data.checkpoint_summary.sequence_number,
                        epoch: data.checkpoint_summary.epoch.try_into().unwrap(),
                        data: event.contents.clone(),
                        index: idx as u64,
                        type_: event.type_.to_string(),
                    };
                    results.push((digest, result));
                }
            }
        }
    }
    return results;
}

#[derive(Serialize,Deserialize,Debug)]
struct BorrowEvent {
    borrower: SuiAddress,
    obligation: ID,
    asset: TypeName,
    amount: u64,
    time: u64
}

#[derive(Serialize,Deserialize,Debug)]
struct BorrowFlashLoanEvent  {
    borrower: SuiAddress,
    asset: TypeName,
    amount: u64
}

// 0xc38f849e81cfe46d4e4320f508ea7dda42934a329d5a6571bb4c3cb6ea63f5da::borrow::BorrowEventV2
#[derive(Serialize,Deserialize,Debug)]
struct BorrowEventV2  {
    borrower: SuiAddress,
    obligation: ID,
    asset: TypeName,
    amount: u64,
    borrow_fee: u64,
    time: u64
}

#[derive(Serialize,Deserialize,Debug)]
struct CollateralDepositEvent {
    provider: SuiAddress,
    obligation: ID,
    deposit_asset: TypeName,
    deposit_amount: u64
}
#[derive(Serialize,Deserialize,Debug)]
struct CollateralWithdrawEvent  {
    taker: SuiAddress,
    obligation: ID,
    withdraw_asset: TypeName,
    withdraw_amount: u64
}

#[derive(Serialize,Deserialize,Debug)]
struct LiquidateEvent  {
    liquidator: SuiAddress,
    obligation: ID,
    debt_type: TypeName,
    collateral_type: TypeName,
    repay_on_behalf: u64,
    repay_revenue: u64,
    liq_amount: u64
}

#[derive(Serialize,Deserialize,Debug)]
struct MintEvent{
    minter: SuiAddress,
    deposit_asset: TypeName,
    deposit_amount: u64,
    mint_asset: TypeName,
    mint_amount: u64,
    time: u64
}

#[derive(Serialize,Deserialize,Debug)]
struct ObligationCreatedEvent {
    sender: SuiAddress,
    obligation: ID,
    obligation_key: ID
}

#[derive(Serialize,Deserialize,Debug)]
struct ObligationLocked  {
    obligation: ID,
    witness: TypeName,
    borrow_locked: bool,
    repay_locked: bool,
    deposit_collateral_locked: bool,
    withdraw_collateral_locked: bool,
    liquidate_locked: bool
}

#[derive(Serialize,Deserialize,Debug)]
struct ObligationUnlocked {
    obligation: ID,
    witness: TypeName

}

#[derive(Serialize,Deserialize,Debug)]
struct RedeemEvent  {
  redeemer: SuiAddress,
    withdraw_asset: TypeName,
    withdraw_amount: u64,
    burn_asset: TypeName,
    burn_amount: u64,
    time: u64
}

#[derive(Serialize,Deserialize,Debug)]
struct RepayEvent {
    repayer: SuiAddress,
    obligation: ID,
    asset: TypeName,
    amount: u64,
    time: u64
}

#[derive(Serialize,Deserialize,Debug)]
struct RepayFlashLoanEvent {
    borrower: SuiAddress,
    asset: TypeName,
    amount: u64
}

#[derive(Serialize,Deserialize,Debug)]
struct TypeName {
    name: String
}