// BYOK key storage in the OS-native keychain (Security.framework / Credential Manager / Secret Service).
// The key never touches a log, an env file, or a tracked file. Redaction helper guards accidental logging.
import { Entry } from "@napi-rs/keyring";

const SERVICE = "codex-byok";

export class SecretStore {
  readonly #service: string;
  constructor(service: string = SERVICE) { this.#service = service; }

  #entry(account: string): Entry { return new Entry(this.#service, account); }

  /** Store a secret under an account name. Overwrites any existing value. */
  set(account: string, secret: string): void {
    if (!secret) throw new Error("refusing to store an empty secret");
    this.#entry(account).setPassword(secret);
  }

  /** Read a secret, or null when absent. Never throws on absence. */
  get(account: string): string | null {
    try { return this.#entry(account).getPassword(); }
    catch { return null; }
  }

  /** Remove a secret. Returns whether something was deleted. */
  delete(account: string): boolean {
    try { return this.#entry(account).deletePassword(); }
    catch { return false; }
  }
}

/** Mask a secret for any diagnostic output: keep a 4-char fingerprint, hide the rest. */
export function redact(secret: string): string {
  if (secret.length <= 8) return "****";
  return `${secret.slice(0, 4)}…${secret.slice(-2)} (${secret.length} chars)`;
}
