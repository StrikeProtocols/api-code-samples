from hashlib import sha256


def compute_settlement_hash(trades):
    content = "|".join(trades[x] for x in sorted(trades.keys()))
    return sha256(str.encode(content)).hexdigest()


settlement_hash = compute_settlement_hash({
    'def456': '1691397c0dd59e172873b77fe6a156a323a1ecd13d00bce16edd0a751599cc09',
    'abc123': '224e51ea4aa4fa8b8e4ae0c6b0417b19f9f02aba761247da2d601532177a2b2a'
})

assert settlement_hash == 'e76b23c00d35eacab62b6bd699149a8156bd53c8cbe17c354f4d52d2b25e2bc5'
