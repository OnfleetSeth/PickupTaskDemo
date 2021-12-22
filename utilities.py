import base64

def encode_b64(to_encode):
    bytes = to_encode.encode('ascii')
    base64_bytes = base64.b64encode(bytes)
    encoded_b64 = base64_bytes.decode('ascii')

    return encoded_b64