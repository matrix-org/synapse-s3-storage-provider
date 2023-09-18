import hashlib
import aws_encryption_sdk
from aws_encryption_sdk.identifiers import EncryptionKeyType, WrappingAlgorithm, CommitmentPolicy
from aws_encryption_sdk.key_providers.raw import RawMasterKeyProvider
from aws_encryption_sdk.internal.crypto.wrapping_keys import WrappingKey

class ClientSideEncryption():
    def __init__(self, master_key):
        self._key_provider = FixedKeyProvider()
        self._key_provider.set_master_key(master_key)

        self._encryption_client

    def decrypt(self, source):
        return self._encryption_client.stream(
            mode='d',
            source=source,
            key_provider=self._key_provider
        )

    def encrypt(self, source):
        return self._encryption_client.stream(
              mode='e',
              source=source,
              key_provider=self._key_provider
        )

class FixedKeyProvider(RawMasterKeyProvider):
    provider_id = "fixed"

    def set_master_key(self, master_key):
        self.master_key = hashlib.sha256(master_key.encode("utf-8")).digest()
        self.add_master_key('')

    def _get_raw_key(self, key_id):
        return WrappingKey(
            wrapping_algorithm=WrappingAlgorithm.AES_256_GCM_IV12_TAG16_NO_PADDING,
            wrapping_key=self.master_key,
            wrapping_key_type=EncryptionKeyType.SYMMETRIC,
        )
