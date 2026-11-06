import base64
import os
import secrets

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

def get_key():
    key_text = os.environ.get("CNA1_SHARED_KEY")
    
    if key_text is None:
        raise ValueError("CNA1_SHARED_KEY is not set")
    
    key = base64.b64decode(key_text)
    
    return key

def encrypt_payload(data):
	key = get_key()
	nonce = secrets.token_bytes(12)
	
	encrypted_data = AESGCM(key).encrypt(nonce, data, None)
 
	return nonce + encrypted_data


def decrypt_payload(data):
	key = get_key()
	
	nonce = data[:12]
	encrypted_data = data[12:]
 
	return AESGCM(key).decrypt(nonce, encrypted_data, None)