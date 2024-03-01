use serde::{Deserialize, Serialize};
use sui_types::base_types::SuiAddress;
use sui_types::id::ID;

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