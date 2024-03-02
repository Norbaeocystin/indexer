use bcs::from_bytes;
use log::warn;
use serde::{Deserialize, Serialize};
use sui_types::base_types::{ObjectID, SuiAddress};
use sui_types::full_checkpoint_content::CheckpointData;
use sui_types::id::ID;

#[derive(Serialize,Deserialize,Debug)]
pub enum ScallopEvent {
    BorrowEvent(BorrowEvent),
    BorrowEventV2(BorrowEventV2),
    BorrowFlashLoanEvent(BorrowFlashLoanEvent),
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
pub fn parse(bytes: &[u8], type_: &str) -> Option<(ScallopEvent, String, Option<String>)> {
    let result = type_.split("::").last().unwrap();
    match result {
        "BorrowEvent" => {
            let event = from_bytes::<BorrowEvent>(&bytes).unwrap();
            return Some((ScallopEvent::BorrowEvent(event.clone()),  result.to_string(), Some(event.obligation.bytes.to_string())));
        }
        "BorrowFlashLoanEvent" => {
            let event = from_bytes::<BorrowFlashLoanEvent>(&bytes).unwrap();
            return Some((ScallopEvent::BorrowFlashLoanEvent(event), result.to_string(), None));
        }
        "BorrowEventV2" => {
            let event = from_bytes::<BorrowEventV2>(&bytes).unwrap();
            return Some((ScallopEvent::BorrowEventV2(event.clone()), result.to_string(), Some(event.obligation.bytes.to_string()) ));
        }
        "CollateralDepositEvent" => {
            let event = from_bytes::<CollateralDepositEvent>(&bytes).unwrap();
            return Some((ScallopEvent::CollateralDepositEvent(event.clone()), result.to_string(), Some(event.obligation.bytes.to_string())));
        }
        "CollateralWithdrawEvent" => {
            let event = from_bytes::<CollateralWithdrawEvent>(&bytes).unwrap();
            return Some((ScallopEvent::CollateralWithdrawEvent(event.clone()), result.to_string(), Some(event.obligation.bytes.to_string())));
        }
        "LiquidateEvent" => {
            let event = from_bytes::<LiquidateEvent>(&bytes).unwrap();
            return Some((ScallopEvent::LiquidateEvent(event.clone()), result.to_string(), Some(event.obligation.bytes.to_string())));
        }
        "MintEvent" => {
            let event = from_bytes::<MintEvent>(&bytes).unwrap();
            return Some((ScallopEvent::MintEvent(event), result.to_string(), None ));
        }
        "ObligationCreatedEvent" => {
            let event = from_bytes::<ObligationCreatedEvent>(&bytes).unwrap();
            return Some((ScallopEvent::ObligationCreatedEvent(event.clone()), result.to_string(), Some(event.obligation.bytes.to_string())));
        }
        "ObligationLocked" => {
            let event = from_bytes::<ObligationLocked>(&bytes).unwrap();
            return Some((ScallopEvent::ObligationLocked(event.clone()), result.to_string(), Some(event.obligation.bytes.to_string()) ));
        }
        "ObligationUnlocked" => {
            let event = from_bytes::<ObligationUnlocked>(&bytes).unwrap();
            return Some((ScallopEvent::ObligationUnlocked(event.clone()), result.to_string(), Some(event.obligation.bytes.to_string())));
        }
        "RedeemEvent" => {
            let event = from_bytes::<RedeemEvent>(&bytes).unwrap();
            return Some((ScallopEvent::RedeemEvent(event),result.to_string(), None));
        }
        "RepayEvent" => {
            let event = from_bytes::<RepayEvent>(&bytes).unwrap();
            return Some((ScallopEvent::RepayEvent(event.clone()),result.to_string(), Some(event.obligation.bytes.to_string())));
        }
        "RepayFlashLoanEvent" => {
            let event = from_bytes::<RepayFlashLoanEvent>(&bytes).unwrap();
            return Some((ScallopEvent::RepayFlashLoanEvent(event), result.to_string(), None));
        }
        _ => {
            warn!("pattern for parsing event not found ...")
        }
    }
    return None;
}

#[derive(Serialize,Deserialize,Debug, Clone)]
pub struct IndexerData {
    pub digest: String,
    pub checkpoint: u64,
    pub epoch: u64,
    pub data: Vec<u8>,
    pub index: u64,
    pub type_: String,
}

impl IndexerData {
    pub fn parse_event(&self) -> Option<(ScallopEvent, String, Option<String>)> {
        return parse(&self.data, &self.type_);
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

#[derive(Serialize,Deserialize,Debug,Clone)]
pub struct BorrowEvent {
    pub borrower: SuiAddress,
    pub obligation: ID,
    pub asset: TypeName,
    pub amount: u64,
   pub time: u64
}

#[derive(Serialize,Deserialize,Debug,Clone)]
pub struct BorrowFlashLoanEvent  {
    pub borrower: SuiAddress,
    pub asset: TypeName,
    pub amount: u64
}

// 0xc38f849e81cfe46d4e4320f508ea7dda42934a329d5a6571bb4c3cb6ea63f5da::borrow::BorrowEventV2
#[derive(Serialize,Deserialize,Debug,Clone)]
pub struct BorrowEventV2  {
    pub borrower: SuiAddress,
    pub obligation: ID,
    pub asset: TypeName,
    pub amount: u64,
    pub borrow_fee: u64,
    pub time: u64
}

#[derive(Serialize,Deserialize,Debug,Clone)]
pub struct CollateralDepositEvent {
    pub provider: SuiAddress,
    pub obligation: ID,
    pub deposit_asset: TypeName,
    pub deposit_amount: u64
}
#[derive(Serialize,Deserialize,Debug,Clone)]
pub struct CollateralWithdrawEvent  {
    pub taker: SuiAddress,
    pub obligation: ID,
    pub withdraw_asset: TypeName,
    pub withdraw_amount: u64
}

#[derive(Serialize,Deserialize,Debug,Clone)]
pub struct LiquidateEvent  {
    pub liquidator: SuiAddress,
    pub obligation: ID,
    pub debt_type: TypeName,
    pub collateral_type: TypeName,
    pub repay_on_behalf: u64,
    pub repay_revenue: u64,
    pub liq_amount: u64
}

#[derive(Serialize,Deserialize,Debug,Clone)]
pub struct MintEvent{
    pub minter: SuiAddress,
    pub deposit_asset: TypeName,
    pub deposit_amount: u64,
    pub mint_asset: TypeName,
    pub mint_amount: u64,
    pub time: u64
}

#[derive(Serialize,Deserialize,Debug,Clone)]
pub struct ObligationCreatedEvent {
    pub sender: SuiAddress,
    pub obligation: ID,
    pub obligation_key: ID
}

#[derive(Serialize,Deserialize,Debug,Clone)]
pub struct ObligationLocked  {
    pub obligation: ID,
    pub witness: TypeName,
    pub borrow_locked: bool,
    pub repay_locked: bool,
    pub deposit_collateral_locked: bool,
    pub withdraw_collateral_locked: bool,
    pub liquidate_locked: bool
}

#[derive(Serialize,Deserialize,Debug,Clone)]
pub struct ObligationUnlocked {
    pub obligation: ID,
    pub witness: TypeName

}

#[derive(Serialize,Deserialize,Debug,Clone)]
pub struct RedeemEvent  {
  pub redeemer: SuiAddress,
  pub  withdraw_asset: TypeName,
  pub  withdraw_amount: u64,
   pub burn_asset: TypeName,
    pub burn_amount: u64,
    pub time: u64
}

#[derive(Serialize,Deserialize,Debug, Clone)]
pub struct RepayEvent {
    pub repayer: SuiAddress,
    pub obligation: ID,
    pub asset: TypeName,
    pub amount: u64,
    pub time: u64
}

#[derive(Serialize,Deserialize,Debug,Clone)]
pub struct RepayFlashLoanEvent {
    pub borrower: SuiAddress,
    pub asset: TypeName,
    pub amount: u64
}

#[derive(Serialize,Deserialize,Debug,Clone)]
pub struct TypeName {
    pub name: String
}