//! PEM parsing through the maintained rustls-pki-types API.
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use std::io;

pub(crate) fn certs(
    reader: &mut dyn io::BufRead,
) -> impl Iterator<Item = io::Result<CertificateDer<'static>>> + '_ {
    CertificateDer::pem_reader_iter(reader).map(|cert| cert.map_err(io::Error::other))
}

pub(crate) fn private_key(
    reader: &mut dyn io::BufRead,
) -> io::Result<Option<PrivateKeyDer<'static>>> {
    PrivateKeyDer::pem_reader_iter(reader)
        .next()
        .transpose()
        .map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reads_certificate_chains_and_private_keys_from_mixed_pem() {
        let identity = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let pem = format!(
            "{}{}{}",
            identity.cert.pem(),
            identity.key_pair.serialize_pem(),
            identity.cert.pem()
        );
        let chain = certs(&mut pem.as_bytes())
            .collect::<io::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0].as_ref(), identity.cert.der().as_ref());
        assert!(private_key(&mut pem.as_bytes()).unwrap().is_some());
        assert!(private_key(&mut identity.cert.pem().as_bytes())
            .unwrap()
            .is_none());
    }
    #[test]
    fn rejects_invalid_pem_data() {
        let pem = b"-----BEGIN CERTIFICATE-----\n!invalid!\n-----END CERTIFICATE-----\n";
        assert!(certs(&mut pem.as_slice()).next().unwrap().is_err());
    }
}
