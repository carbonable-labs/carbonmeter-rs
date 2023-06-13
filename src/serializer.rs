pub fn serialize_to_bytes(val: &[&str]) -> Vec<u8> {
    (*val.join(",").as_bytes()).to_vec()
}

pub fn deserialize_from_bytes(val: &Vec<u8>) -> Vec<&str> {
    let str = std::str::from_utf8(val.as_slice()).expect("should be valid utf-8 string");
    str.split(',').collect()
}

#[cfg(test)]
mod tests {
    use crate::serializer::{deserialize_from_bytes, serialize_to_bytes};

    #[test]
    fn test_serialize_to_bytes() {
        let hex_addr = ["a", "b", "c"];
        let res = serialize_to_bytes(&hex_addr);
        assert_eq!(vec![97, 44, 98, 44, 99], res);
    }

    #[test]
    fn test_deserialize_to_string() {
        let hex_addr = vec![97, 44, 98, 44, 99];

        let res = deserialize_from_bytes(&hex_addr);
        assert_eq!(vec!["a", "b", "c"], res);
    }
}
