const F_MAKER: usize = 1;
const F_TOKEN: usize = 3;
const F_MAKER_AMT: usize = 4;
const F_TAKER_AMT: usize = 5;
const F_SIDE: usize = 6;
pub const MATCH_SELECTOR: [u8; 4] = [0x3c, 0x2b, 0x43, 0x99];
#[derive(Debug, Clone, PartialEq)]
pub struct Decoded {
    pub condition_id: [u8; 32],
    pub token_id: String,
    pub side: u8,
    pub price: f64,
    pub order_size: f64,
    pub fill_size: f64,
    pub role: &'static str,
    pub salt: [u8; 32],
    pub occurrence: u16,
}
pub const TAKER_OCCURRENCE: u16 = u16::MAX;
#[inline]
fn word(b: &[u8], off: usize) -> Option<&[u8]> {
    b.get(off..)?.get(..32)
}
#[inline]
fn word_usize(b: &[u8], off: usize) -> Option<usize> {
    let w = word(b, off)?;
    if w[..24].iter().any(|&x| x != 0) {
        return None;
    }
    let mut v = 0usize;
    for &byte in &w[24..] {
        v = v.checked_mul(256)?.checked_add(byte as usize)?;
    }
    Some(v)
}
#[inline]
fn word_u128(b: &[u8], off: usize) -> Option<u128> {
    let w = word(b, off)?;
    if w[..16].iter().any(|&x| x != 0) {
        return None;
    }
    let mut v = [0u8; 16];
    v.copy_from_slice(&w[16..]);
    Some(u128::from_be_bytes(v))
}
fn word_dec_string(b: &[u8], off: usize) -> Option<String> {
    let w = word(b, off)?;
    let mut digits: Vec<u8> = Vec::with_capacity(78);
    let mut acc = w.to_vec();
    if acc.iter().all(|&x| x == 0) {
        return Some("0".into());
    }
    while acc.iter().any(|&x| x != 0) {
        let mut rem = 0u32;
        for byte in acc.iter_mut() {
            let cur = rem * 256 + *byte as u32;
            *byte = (cur / 10) as u8;
            rem = cur % 10;
        }
        digits.push(b'0' + rem as u8);
    }
    digits.reverse();
    String::from_utf8(digits).ok()
}
#[inline]
fn addr_matches(b: &[u8], off: usize, wallet_20: &[u8; 20]) -> bool {
    match word(b, off) {
        Some(w) => w[..12].iter().all(|&x| x == 0) && &w[12..] == wallet_20,
        None => false,
    }
}
fn shares_and_price(
    b: &[u8],
    ord: usize,
    fill_raw: u128,
) -> Option<(f64, f64, f64, u8)> {
    let order = b.get(ord..)?;
    let maker_amt = word_u128(order, F_MAKER_AMT * 32)?;
    let taker_amt = word_u128(order, F_TAKER_AMT * 32)?;
    let side = word_u128(order, F_SIDE * 32)?;
    if maker_amt == 0 || taker_amt == 0 || side > 1 {
        return None;
    }
    let side = side as u8;
    let (ma, ta) = (maker_amt as f64, taker_amt as f64);
    let price = if side == 0 { ma / ta } else { ta / ma };
    if !(price > 0.0 && price < 1.0) {
        return None;
    }
    let order_size = (if side == 0 { ta } else { ma }) / 1e6;
    let raw = fill_raw as f64 / 1e6;
    let size = if side == 0 { raw / price } else { raw };
    Some((size, price, order_size, side))
}
pub fn participants(calldata: &[u8]) -> Vec<[u8; 20]> {
    let mut out = Vec::new();
    if calldata.len() < 4 + 32 * 7 || calldata[..4] != MATCH_SELECTOR {
        return out;
    }
    let b = &calldata[4..];
    let mut push = |off: usize| {
        if let Some(w) = word(b, off) {
            if w[..12].iter().all(|&x| x == 0) {
                let mut a = [0u8; 20];
                a.copy_from_slice(&w[12..]);
                out.push(a);
            }
        }
    };
    if let Some(t) = word_usize(b, 32).and_then(|off| off.checked_add(F_MAKER * 32)) {
        push(t);
    }
    if let Some(mo) = word_usize(b, 64) {
        if let Some(n) = word_usize(b, mo) {
            if n <= 512 {
                let Some(body) = mo.checked_add(32) else { return out };
                for i in 0..n {
                    let Some(index) = body.checked_add(i * 32) else { continue };
                    let Some(rel) = word_usize(b, index) else { continue };
                    let Some(maker) = body.checked_add(rel)
                        .and_then(|off| off.checked_add(F_MAKER * 32)) else { continue };
                    push(maker);
                }
            }
        }
    }
    out
}
pub fn decode_all(calldata: &[u8], wallet_20: &[u8; 20]) -> Vec<Decoded> {
    match decode_all_inner(calldata, wallet_20) {
        Some(v) => v,
        None => Vec::new(),
    }
}
fn decode_all_inner(calldata: &[u8], wallet_20: &[u8; 20]) -> Option<Vec<Decoded>> {
    if calldata.len() < 4 + 32 * 7 || calldata[..4] != MATCH_SELECTOR {
        return None;
    }
    let b = &calldata[4..];
    let mut condition_id = [0u8; 32];
    condition_id.copy_from_slice(word(b, 0)?);
    if condition_id.iter().all(|&x| x == 0) {
        return None;
    }
    let taker_order_off = word_usize(b, 32)?;
    let maker_orders_off = word_usize(b, 64)?;
    let taker_fill = word_u128(b, 96)?;
    let maker_fills_off = word_usize(b, 128)?;
    let n_fills = word_usize(b, maker_fills_off)?;
    let n_makers = word_usize(b, maker_orders_off)?;
    if n_makers > 512 || n_fills > 512 {
        return None;
    }
    let makers_body = maker_orders_off.checked_add(32)?;
    let fills_body = maker_fills_off.checked_add(32)?;
    let mut out: Vec<Decoded> = Vec::new();
    let push = |
        ord: usize,
        fill_raw: u128,
        role: &'static str,
        occurrence: u16,
        out: &mut Vec<Decoded>,
    | -> Option<()> {
        if fill_raw == 0 {
            return None;
        }
        let (size, price, order_size, side) = shares_and_price(b, ord, fill_raw)?;
        let order = b.get(ord..)?;
        let mut salt = [0u8; 32];
        salt.copy_from_slice(word(order, 0)?);
        let token_id = word_dec_string(order, F_TOKEN * 32)?;
        if token_id.len() < 6 {
            return None;
        }
        if size <= 0.0 || size > order_size * 1.001 {
            return None;
        }
        if salt.iter().all(|&x| x == 0) {
            return None;
        }
        out.push(Decoded {
            condition_id,
            token_id,
            side,
            price,
            order_size,
            fill_size: size,
            role,
            salt,
            occurrence,
        });
        Some(())
    };
    for i in 0..n_makers {
        let Some(rel) = word_usize(b, makers_body.checked_add(i * 32)?) else { continue };
        let ord = makers_body.checked_add(rel)?;
        if !addr_matches(b, ord.checked_add(F_MAKER * 32)?, wallet_20) {
            continue;
        }
        if i >= n_fills {
            continue;
        }
        let Some(fill) = word_u128(b, fills_body.checked_add(i * 32)?) else { continue };
        let _ = push(ord, fill, "maker", i as u16, &mut out);
    }
    if addr_matches(b, taker_order_off.checked_add(F_MAKER * 32)?, wallet_20) {
        let _ = push(taker_order_off, taker_fill, "taker", TAKER_OCCURRENCE, &mut out);
    }
    Some(out)
}
pub fn decode_pending(calldata: &[u8], wallet_20: &[u8; 20]) -> Option<Decoded> {
    let mut all = decode_all(calldata, wallet_20);
    if all.len() == 1 { all.pop() } else { None }
}


/// V2 OrderFilled bytes: [maker side, token ID, maker fill amount, taker fill
/// amount, fee metadata]. The first word is **side** (0 BUY, 1 SELL), not an
/// asset ID. The indexed maker topic authenticates the order owner. A taker
/// topic alone is only counterparty evidence and MUST NOT authorize copying.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderFilled {
    pub maker: [u8; 20],
    pub token_id: String,
    pub side: u8,
    pub shares_micro: u128,
    pub quote_micro: u128,
    pub log_index: u64,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ConfirmedLeaderFill {
    pub wallet: [u8; 20],
    pub transaction_hash: String,
    pub block_time: u64,
    pub log_index: u64,
    /// Same admission type as the existing matchOrders decoder; price and
    /// filled shares are taken from the actual settled OrderFilled event.
    pub order: Decoded,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillDetectionError {
    InvalidOrderFilled,
    MissingSignedMakerOrder,
    AmbiguousSignedMakerOrder,
    UnverifiableAmount,
}

pub fn decode_v2_order_filled(
    data: &[u8], maker: [u8; 20], log_index: u64,
) -> Result<OrderFilled, FillDetectionError> {
    if data.len() != 224 { return Err(FillDetectionError::InvalidOrderFilled); }
    let side = word_usize(data, 0).ok_or(FillDetectionError::InvalidOrderFilled)?;
    if side > 1 { return Err(FillDetectionError::InvalidOrderFilled); }
    let token_id = word_dec_string(data, 32).ok_or(FillDetectionError::InvalidOrderFilled)?;
    if token_id == "0" { return Err(FillDetectionError::InvalidOrderFilled); }
    let ma = word_u128(data, 64).ok_or(FillDetectionError::InvalidOrderFilled)?;
    let ta = word_u128(data, 96).ok_or(FillDetectionError::InvalidOrderFilled)?;
    let (quote, shares) = if side == 0 { (ma, ta) } else { (ta, ma) };
    if shares == 0 || quote == 0 || quote >= shares {
        return Err(FillDetectionError::UnverifiableAmount);
    }
    Ok(OrderFilled {maker, token_id, side:side as u8,
        shares_micro:shares, quote_micro:quote, log_index})
}

/// A canonical topic only authenticates a maker *address*. Pair it with the
/// actual signed maker order in matchOrders; counterparty taker topics do not
/// establish the leader's intention. Preserve the real order salt/condition,
/// but use settled fill amount and price rather than the order's limit.
pub fn verified_order_fills(
    calldata: &[u8], wallet: &[u8; 20], events: &[OrderFilled],
    tx_hash: &str, block_time: u64,
) -> Result<Vec<ConfirmedLeaderFill>, FillDetectionError> {
    let orders = decode_all(calldata, wallet);
    if orders.is_empty() || block_time == 0 {
        return Err(FillDetectionError::MissingSignedMakerOrder);
    }
    let mut result = Vec::with_capacity(orders.len());
    for mut order in orders {
        if result.iter().any(|x: &ConfirmedLeaderFill| {
            x.order.token_id == order.token_id && x.order.side == order.side
        }) {
            return Err(FillDetectionError::AmbiguousSignedMakerOrder);
        }
        let matching: Vec<_> = events.iter().filter(|event| {
            &event.maker == wallet && event.token_id == order.token_id
                && event.side == order.side
        }).collect();
        if matching.is_empty() {return Err(FillDetectionError::MissingSignedMakerOrder);}
        let mut shares: u128 = 0;
        let mut quote: u128 = 0;
        for event in &matching {
            shares = shares.checked_add(event.shares_micro).ok_or(FillDetectionError::UnverifiableAmount)?;
            quote = quote.checked_add(event.quote_micro).ok_or(FillDetectionError::UnverifiableAmount)?;
        }
        let size = shares as f64 / 1e6;
        let price = quote as f64 / shares as f64;
        if !size.is_finite() || !price.is_finite() || !(0.0..1.0).contains(&price) {
            return Err(FillDetectionError::UnverifiableAmount);
        }
        order.fill_size = size;
        order.price = price;
        // A better execution price may fill more shares than implied by the
        // signed limit price. Never reject a real settled fill as oversized.
        order.order_size = order.order_size.max(size);
        result.push(ConfirmedLeaderFill {
            wallet:*wallet, transaction_hash:tx_hash.to_owned(), block_time,
            log_index:matching.iter().map(|x| x.log_index).min().unwrap_or(0),
            order,
        });
    }
    Ok(result)
}

#[cfg(test)]
mod settled_log_tests {
    use super::*;
    fn put_word(data: &mut [u8], offset:usize, value:u128) {
        data[offset+16..offset+32].copy_from_slice(&value.to_be_bytes());
    }
    #[test]
    fn settled_event_joins_signed_calldata_and_replaces_limit_price() {
        let maker=[0x22;20];
        let mut abi=vec![0u8;448];
        put_word(&mut abi,0,1); // nonzero condition
        put_word(&mut abi,32,160); // taker order at word5
        put_word(&mut abi,64,384); // empty maker orders
        put_word(&mut abi,96,5_000_000); // taker quote fill
        put_word(&mut abi,128,416); // empty maker fill list
        put_word(&mut abi,160,2); // real signed order salt
        abi[204..224].copy_from_slice(&maker); // ABI padded maker
        put_word(&mut abi,256,123456); // token ID
        put_word(&mut abi,288,100_000_000); // maker amount
        put_word(&mut abi,320,200_000_000); // taker amount
        let mut calldata=MATCH_SELECTOR.to_vec();calldata.extend(abi);
        let original=decode_all(&calldata,&maker);
        assert_eq!(original.len(),1);
        assert!((original[0].price-0.5).abs()<1e-9);
        let event=OrderFilled {maker,token_id:"123456".into(),side:0,
            shares_micro:10_000_000,quote_micro:4_000_000,log_index:7};
        let settled=verified_order_fills(&calldata,&maker,&[event.clone()],"fixture",100).unwrap();
        assert_eq!(settled.len(),1);
        assert_eq!(settled[0].order.token_id,"123456");
        assert!((settled[0].order.price-0.4).abs()<1e-9);
        assert_eq!(settled[0].order.fill_size,10.0);
        assert_eq!(settled[0].order.salt[31],2);
        assert_eq!(settled[0].block_time,100);
        let another=[0x33;20];
        assert_eq!(verified_order_fills(&calldata,&another,&[event],"fixture",100),
            Err(FillDetectionError::MissingSignedMakerOrder));
    }
    #[test]
    fn v2_buy_and_sell_are_typed_and_invalid_rows_refuse() {
        let maker=[0x22;20];let mut b=vec![0u8;224];
        put_word(&mut b, 32, 12345);
        put_word(&mut b, 64, 5_000_000);
        put_word(&mut b, 96, 10_000_000);
        let buy=decode_v2_order_filled(&b,maker,7).unwrap();
        assert_eq!((buy.side,buy.token_id.as_str(),buy.shares_micro,buy.quote_micro),(0,"12345",10_000_000,5_000_000));
        put_word(&mut b,0,1);
        put_word(&mut b,64,10_000_000);
        put_word(&mut b,96,5_000_000);
        let sell=decode_v2_order_filled(&b,maker,8).unwrap();
        assert_eq!((sell.side,sell.shares_micro,sell.quote_micro),(1,10_000_000,5_000_000));
        put_word(&mut b,0,2);
        assert_eq!(decode_v2_order_filled(&b,maker,9),Err(FillDetectionError::InvalidOrderFilled));
    }
}
