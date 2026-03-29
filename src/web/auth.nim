import std/[random, strutils]
import nimcrypto

const CONFIG_PASSPHRASE = "BcgGLy>VD}4]P_A2@ukGwA<m8CqAtyL|"

proc getPassphrase*(): string =
  return CONFIG_PASSPHRASE

proc padPkcs7(data: var seq[byte], blockSize: int) =
  let missing = blockSize - (data.len mod blockSize)
  for i in 1..missing:
    data.add(byte(missing))

proc encryptPassword*(password: string): string =
  # --- Step 1: Generate Key & IV (SHA256) ---
  var sha: sha256
  sha.init()
  # Convert config string to bytes manually
  for c in CONFIG_PASSPHRASE:
    sha.update([byte(c)])
  let fullHash = sha.finish().data # array[32, byte]
  sha.clear()

  # Key = Full 32 bytes
  var key = fullHash

  # IV = First 16 bytes
  var iv: array[16, byte]
  for i in 0..15: iv[i] = fullHash[i]

  # --- Step 2: Prepare & Pad Password ---
  var plainBytes = newSeq[byte]()
  for c in password: plainBytes.add(byte(c))

  # Apply PKCS7 Padding manually (Block size 16 for AES)
  padPkcs7(plainBytes, 16)

  # --- Step 3: Encrypt (AES-256-CBC) ---
  var ctx: CBC[aes256]
  var encryptedBytes = newSeq[byte](plainBytes.len) # Output size = Padded Input size

  ctx.init(key, iv)
  ctx.encrypt(plainBytes, encryptedBytes)
  ctx.clear()

  # --- Step 4: Manual Hex Conversion ---
  result = ""
  for b in encryptedBytes:
    result.add(toHex(int(b), 2))

  return result.toLowerAscii()

proc genToken*(): string =
  randomize()
  for i in 0..<32:
    result &= "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".sample()

when isMainModule:
  echo "Encrypted: ", encryptPassword("admin")
