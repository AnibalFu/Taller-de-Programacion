use crate::internal_protocol::protocol_trait::{DeserializeRIP, SerializeRIP};
use std::io::{Error, ErrorKind, Read};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

pub fn read_exact<const N: usize, T: Read>(stream: &mut T) -> Result<[u8; N], Error> {
    let mut buf = [0u8; N];
    stream.read_exact(&mut buf)?;
    Ok(buf)
}

impl SerializeRIP for SocketAddr {
    fn serialize(&self) -> Vec<u8> {
        match self {
            SocketAddr::V4(a) => {
                let mut bytes = vec![4u8];
                bytes.extend(&a.ip().octets());
                bytes.extend(&a.port().to_be_bytes());
                bytes
            }
            SocketAddr::V6(a) => {
                let mut bytes = vec![6u8];
                bytes.extend(&a.ip().octets());
                bytes.extend(&a.port().to_be_bytes());
                bytes
            }
        }
    }
}

impl DeserializeRIP for SocketAddr {
    fn deserialize<T: Read>(stream: &mut T) -> std::io::Result<Self> {
        let kind = read_exact::<1, _>(stream)?[0];

        match kind {
            4 => {
                let ip_bytes = read_exact::<4, _>(stream)?;
                let ip = Ipv4Addr::from(ip_bytes);
                let port_bytes = read_exact::<2, _>(stream)?;
                let port = u16::from_be_bytes(port_bytes);
                Ok(SocketAddr::new(IpAddr::V4(ip), port))
            }
            6 => {
                let ip_bytes = read_exact::<16, _>(stream)?;
                let ip = Ipv6Addr::from(ip_bytes);
                let port_bytes = read_exact::<2, _>(stream)?;
                let port = u16::from_be_bytes(port_bytes);
                Ok(SocketAddr::new(IpAddr::V6(ip), port))
            }
            _ => Err(Error::new(ErrorKind::InvalidData, "Tipo de IP inválido")),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::internal_protocol::protocol_trait::{DeserializeRIP, SerializeRIP};
    use crate::internal_protocol::utils::read_exact;
    use crate::node_id::NodeId;
    use std::io::Cursor;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    #[test]
    fn test01_cursor_read_correctly() {
        let vec: Vec<u8> = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let mut cursor = Cursor::new(vec.clone());

        let lectura = read_exact::<3, _>(&mut cursor).unwrap();
        assert_eq!(&vec[0..3], lectura);
    }

    #[test]
    fn test02_cursor_read_multiple_correctly() {
        let vec: Vec<u8> = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let mut cursor = Cursor::new(vec.clone());

        let lectura1 = read_exact::<3, _>(&mut cursor).unwrap();
        let lectura2 = read_exact::<3, _>(&mut cursor).unwrap();

        assert_eq!(&vec[0..3], lectura1);
        assert_eq!(&vec[3..6], lectura2);
    }

    #[test]
    fn test03_from_bytes_node_id_cursor() {
        let id = "1234567890abcdef1234567890abcdef12345678";
        let mut cursor: Cursor<Vec<u8>> = Cursor::new(Vec::from(id.as_bytes()));

        let result = NodeId::deserialize(&mut cursor).unwrap();
        assert_eq!(result.get_id(), id);
    }

    #[test]
    fn test04_deserialize_socketaddr_ipv4() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10)), 8080);
        let encoded = addr.serialize();
        let mut cursor = Cursor::new(encoded);

        let result = SocketAddr::deserialize(&mut cursor).unwrap();
        assert_eq!(result, addr);
    }

    #[test]
    fn test05_deserialize_socketaddr_ipv6() {
        let addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 12345);
        let encoded = addr.serialize();
        let mut cursor = Cursor::new(encoded);

        let result = SocketAddr::deserialize(&mut cursor).unwrap();
        assert_eq!(result, addr);
    }
}
