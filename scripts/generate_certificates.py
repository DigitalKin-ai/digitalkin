#!/usr/bin/env python3
"""Certificate generation utility for gRPC secure mode and mTLS testing.

This script generates all necessary certificates for testing gRPC servers
in secure mode (TLS) and mutual TLS (mTLS) configurations.
"""

import argparse
import datetime
import ipaddress
import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

try:
    from cryptography import x509
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
except ImportError:
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
class CertificateConfig:
    """Configuration for certificate generation.

    Attributes:
        output_dir: Directory where certificates will be saved
        key_size: Size of the RSA keys in bits
        days_valid: Number of days the certificates will be valid
        ca_common_name: Common name for the CA certificate
        ca_country: Country code for the CA certificate
        ca_organization: Organization name for the CA certificate
        server_common_name: Common name for the server certificate
        server_country: Country code for the server certificate
        server_organization: Organization name for the server certificate
        server_dns_names: List of DNS names for the server certificate
        server_ip_addresses: List of IP addresses for the server certificate
        client_common_name: Common name for the client certificate
        client_country: Country code for the client certificate
        client_organization: Organization name for the client certificate
    """

    # Common settings
    output_dir: Path = Path("./certs")
    key_size: int = 2048
    days_valid: int = 365

    # CA settings
    ca_common_name: str = "Test CA"
    ca_country: str = "US"
    ca_organization: str = "Test Organization"

    # Server settings
    server_common_name: str = "localhost"
    server_country: str = "US"
    server_organization: str = "Test Server"
    server_dns_names: list[str] = field(default_factory=lambda: ["localhost"])
    server_ip_addresses: list[str] = field(default_factory=lambda: ["127.0.0.1"])

    # Client settings
    client_common_name: str = "Test Client"
    client_country: str = "US"
    client_organization: str = "Test Client"

    def __post_init__(self) -> None:
        """Create output directory if it doesn't exist."""
        self.output_dir.mkdir(parents=True, exist_ok=True)


def generate_private_key(key_size: int) -> rsa.RSAPrivateKey:
    """Generate an RSA private key.

    Args:
        key_size: Size of the key in bits.

    Returns:
        A new RSA private key.
    """
    return rsa.generate_private_key(
        public_exponent=65537,  # Standard value for exponent
        key_size=key_size,
        backend=default_backend(),
    )


def generate_ca_certificate(config: CertificateConfig) -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    """Generate a CA certificate and private key.

    Args:
        config: Certificate configuration.

    Returns:
        A tuple containing the CA private key and certificate.
    """
    # Generate a private key
    private_key = generate_private_key(config.key_size)

    # Create a name for the CA
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, config.ca_common_name),
        x509.NameAttribute(NameOID.COUNTRY_NAME, config.ca_country),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, config.ca_organization),
    ])

    # Build the CA certificate
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .not_valid_before(datetime.datetime.utcnow())
        .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=config.days_valid))
        .serial_number(x509.random_serial_number())
        .public_key(private_key.public_key())
        .add_extension(
            x509.BasicConstraints(ca=True, path_length=None),
            critical=True,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(private_key, hashes.SHA256(), default_backend())
    )

    return private_key, ca_cert


def generate_certificate(
    config: CertificateConfig,
    ca_key: rsa.RSAPrivateKey,
    ca_cert: x509.Certificate,
    is_server: bool = True,
) -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    """Generate a certificate signed by the CA.

    Args:
        config: Certificate configuration.
        ca_key: CA private key.
        ca_cert: CA certificate.
        is_server: Whether to generate a server or client certificate.

    Returns:
        A tuple containing the private key and certificate.
    """
    # Generate a private key
    private_key = generate_private_key(config.key_size)

    # Use server or client configuration
    if is_server:
        common_name = config.server_common_name
        country = config.server_country
        organization = config.server_organization
    else:
        common_name = config.client_common_name
        country = config.client_country
        organization = config.client_organization

    # Create a name for the certificate
    subject = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, common_name),
        x509.NameAttribute(NameOID.COUNTRY_NAME, country),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, organization),
    ])

    # Start building the certificate
    cert_builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_cert.subject)
        .not_valid_before(datetime.datetime.utcnow())
        .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=config.days_valid))
        .serial_number(x509.random_serial_number())
        .public_key(private_key.public_key())
    )

    # Add specific extensions for server or client
    if is_server:
        # Add DNS names and IP addresses for the server

        san_list = [x509.DNSName(dns_name) for dns_name in config.server_dns_names]

        san_list.extend(x509.IPAddress(ipaddress.ip_address(ip_address)) for ip_address in config.server_ip_addresses)

        cert_builder = cert_builder.add_extension(
            x509.SubjectAlternativeName(san_list),
            critical=False,
        )

        cert_builder = cert_builder.add_extension(
            x509.ExtendedKeyUsage([
                x509.oid.ExtendedKeyUsageOID.SERVER_AUTH,
            ]),
            critical=False,
        )
    else:
        # Add client authentication key usage
        cert_builder = cert_builder.add_extension(
            x509.ExtendedKeyUsage([
                x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH,
            ]),
            critical=False,
        )

    # Common extensions for both server and client
    cert_builder = cert_builder.add_extension(
        x509.BasicConstraints(ca=False, path_length=None),
        critical=True,
    )

    cert_builder = cert_builder.add_extension(
        x509.KeyUsage(
            digital_signature=True,
            content_commitment=False,
            key_encipherment=True,
            data_encipherment=False,
            key_agreement=False,
            key_cert_sign=False,
            crl_sign=False,
            encipher_only=False,
            decipher_only=False,
        ),
        critical=True,
    )

    # Sign the certificate with CA key
    certificate = cert_builder.sign(ca_key, hashes.SHA256(), default_backend())

    return private_key, certificate


def save_private_key(key: rsa.RSAPrivateKey, path: Path, password: str | None = None) -> None:
    """Save a private key to a file.

    Args:
        key: The private key to save.
        path: The path where to save the key.
        password: Optional password to encrypt the key.
    """
    encryption = None
    if password:
        encryption = serialization.BestAvailableEncryption(password.encode())

    with open(path, "wb") as f:
        f.write(
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=encryption or serialization.NoEncryption(),
            )
        )

    # Set restrictive permissions for private key
    os.chmod(path, 0o600)  # Read/write for owner only

    logger.info(f"Private key saved to {path}")


def save_certificate(cert: x509.Certificate, path: Path) -> None:
    """Save a certificate to a file.

    Args:
        cert: The certificate to save.
        path: The path where to save the certificate.
    """
    with open(path, "wb") as f:
        f.write(
            cert.public_bytes(
                encoding=serialization.Encoding.PEM,
            )
        )

    logger.info(f"Certificate saved to {path}")


def generate_certificates(config: CertificateConfig) -> None:
    """Generate CA, server, and client certificates.

    Args:
        config: Certificate configuration.
    """
    # Generate CA certificate
    logger.info("Generating CA certificate...")
    ca_key, ca_cert = generate_ca_certificate(config)

    ca_key_path = config.output_dir / "ca.key"
    ca_cert_path = config.output_dir / "ca.crt"

    save_private_key(ca_key, ca_key_path)
    save_certificate(ca_cert, ca_cert_path)

    # Generate server certificate
    logger.info("Generating server certificate...")
    server_key, server_cert = generate_certificate(config, ca_key, ca_cert, is_server=True)

    server_key_path = config.output_dir / "server.key"
    server_cert_path = config.output_dir / "server.crt"

    save_private_key(server_key, server_key_path)
    save_certificate(server_cert, server_cert_path)

    # Generate client certificate
    logger.info("Generating client certificate...")
    client_key, client_cert = generate_certificate(config, ca_key, ca_cert, is_server=False)

    client_key_path = config.output_dir / "client.key"
    client_cert_path = config.output_dir / "client.crt"

    save_private_key(client_key, client_key_path)
    save_certificate(client_cert, client_cert_path)

    logger.info("Certificate generation complete!")
    logger.info(f"All certificates and keys saved to {config.output_dir}")

    # Print example usage for gRPC


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Generate certificates for gRPC secure mode testing")
    parser.add_argument(
        "--output-dir",
        "-o",
        type=Path,
        default=Path("./certs"),
        help="Directory where certificates will be saved (default: ./certs)",
    )
    parser.add_argument(
        "--key-size",
        "-k",
        type=int,
        default=2048,
        choices=[1024, 2048, 4096],
        help="Size of the RSA keys in bits (default: 2048)",
    )
    parser.add_argument(
        "--days-valid", "-d", type=int, default=365, help="Number of days the certificates will be valid (default: 365)"
    )
    parser.add_argument(
        "--server-name",
        "-s",
        type=str,
        default="localhost",
        help="Common name for the server certificate (default: localhost)",
    )
    parser.add_argument(
        "--dns-names",
        "-n",
        type=str,
        nargs="+",
        default=["localhost"],
        help="DNS names for the server certificate (default: localhost)",
    )
    parser.add_argument(
        "--ip-addresses",
        "-i",
        type=str,
        nargs="+",
        default=["127.0.0.1"],
        help="IP addresses for the server certificate (default: 127.0.0.1)",
    )

    return parser.parse_args()


def main() -> None:
    """Main function."""
    try:
        args = parse_args()

        config = CertificateConfig(
            output_dir=args.output_dir,
            key_size=args.key_size,
            days_valid=args.days_valid,
            server_common_name=args.server_name,
            server_dns_names=args.dns_names,
            server_ip_addresses=args.ip_addresses,
        )

        generate_certificates(config)

    except Exception as e:
        logger.exception(f"Certificate generation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
