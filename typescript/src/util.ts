export const encode = (text: string) => new TextEncoder().encode(text)

export const decode = (content: Uint8Array) => new TextDecoder().decode(content)

export const toHex = (bytes: Uint8Array) =>
  Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('')

export const errorMessage = (error: unknown) =>
  error instanceof Error ? error.message : String(error)
