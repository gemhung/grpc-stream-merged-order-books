use serde::de;
use serde::{Deserialize, Deserializer};

#[derive(Debug, Deserialize)]
pub struct OfferData {
    #[serde(deserialize_with = "de_float_from_str")]
    pub price: f64,
    #[serde(deserialize_with = "de_float_from_str")]
    pub qty: f64,
}

#[derive(Debug, Deserialize)]
pub struct BitstampBook {
    pub timestamp: String,
    pub microtimestamp: String,
    pub bids: Vec<OfferData>,
    pub asks: Vec<OfferData>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum BitstampData {
    Book(BitstampBook),
    None {},
}

#[derive(Debug, Deserialize)]
pub struct BitsMap {
    pub channel: String,
    pub event: String,
    pub data: BitstampData,
}

pub fn de_float_from_str<'a, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'a>,
{
    let str_val = String::deserialize(deserializer)?;
    str_val.parse::<f64>().map_err(de::Error::custom)
}
