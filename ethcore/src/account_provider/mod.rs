// Copyright 2015, 2016 Parity Technologies (UK) Ltd.
// This file is part of Parity.

// Parity is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Parity is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Parity.  If not, see <http://www.gnu.org/licenses/>.

//! Account management.

mod stores;

use self::stores::{AddressBook, DappsSettingsStore};

use std::fmt;
use std::collections::HashMap;
use std::time::{Instant, Duration};
use util::{Mutex, RwLock};
use ethstore::{SecretStore, Error as SSError, SafeAccount, EthStore};
use ethstore::dir::{KeyDirectory};
use ethstore::ethkey::{Address, Message, Public, Secret, Random, Generator};
use ethjson::misc::AccountMeta;
pub use ethstore::ethkey::Signature;

/// Type of unlock.
#[derive(Clone)]
enum Unlock {
	/// If account is unlocked temporarily, it should be locked after first usage.
	Temp,
	/// Account unlocked permantently can always sign message.
	/// Use with caution.
	Perm,
	/// Account unlocked with a timeout
	Timed(Instant),
}

/// Data associated with account.
#[derive(Clone)]
struct AccountData {
	unlock: Unlock,
	password: String,
}

/// `AccountProvider` errors.
#[derive(Debug)]
pub enum Error {
	/// Returned when account is not unlocked.
	NotUnlocked,
	/// Returned when signing fails.
	SStore(SSError),
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		match *self {
			Error::NotUnlocked => write!(f, "Account is locked"),
			Error::SStore(ref e) => write!(f, "{}", e),
		}
	}
}

impl From<SSError> for Error {
	fn from(e: SSError) -> Self {
		Error::SStore(e)
	}
}

#[derive(Default)]
struct NullDir {
	accounts: RwLock<HashMap<Address, SafeAccount>>,
}

impl KeyDirectory for NullDir {
	fn load(&self) -> Result<Vec<SafeAccount>, SSError> {
		Ok(self.accounts.read().values().cloned().collect())
	}

	fn insert(&self, account: SafeAccount) -> Result<SafeAccount, SSError> {
		self.accounts.write().insert(account.address.clone(), account.clone());
		Ok(account)
	}

	fn remove(&self, address: &Address) -> Result<(), SSError> {
		self.accounts.write().remove(address);
		Ok(())
	}
}

/// Dapp identifier
pub type DappId = String;

/// Account management.
/// Responsible for unlocking accounts.
pub struct AccountProvider {
	unlocked: Mutex<HashMap<Address, AccountData>>,
	sstore: Box<SecretStore>,
	address_book: RwLock<AddressBook>,
	dapps_settings: RwLock<DappsSettingsStore>,
}

impl AccountProvider {
	/// Creates new account provider.
	pub fn new(sstore: Box<SecretStore>) -> Self {
		AccountProvider {
			unlocked: Mutex::new(HashMap::new()),
			address_book: RwLock::new(AddressBook::new(sstore.local_path().into())),
			dapps_settings: RwLock::new(DappsSettingsStore::new(sstore.local_path().into())),
			sstore: sstore,
		}
	}

	/// Creates not disk backed provider.
	pub fn transient_provider() -> Self {
		AccountProvider {
			unlocked: Mutex::new(HashMap::new()),
			address_book: RwLock::new(AddressBook::transient()),
			dapps_settings: RwLock::new(DappsSettingsStore::transient()),
			sstore: Box::new(EthStore::open(Box::new(NullDir::default()))
				.expect("NullDir load always succeeds; qed"))
		}
	}

	/// Creates new random account.
	pub fn new_account(&self, password: &str) -> Result<Address, Error> {
		self.new_account_and_public(password).map(|d| d.0)
	}

	/// Creates new random account and returns address and public key
	pub fn new_account_and_public(&self, password: &str) -> Result<(Address, Public), Error> {
		let acc = Random.generate().expect("secp context has generation capabilities; qed");
		let public = acc.public().clone();
		let secret = acc.secret().clone();
		let address = try!(self.sstore.insert_account(secret, password));
		Ok((address, public))
	}

	/// Inserts new account into underlying store.
	/// Does not unlock account!
	pub fn insert_account(&self, secret: Secret, password: &str) -> Result<Address, Error> {
		let address = try!(self.sstore.insert_account(secret, password));
		Ok(address)
	}

	/// Import a new presale wallet.
	pub fn import_presale(&self, presale_json: &[u8], password: &str) -> Result<Address, Error> {
		let address = try!(self.sstore.import_presale(presale_json, password));
		Ok(Address::from(address).into())
	}

	/// Import a new presale wallet.
	pub fn import_wallet(&self, json: &[u8], password: &str) -> Result<Address, Error> {
		let address = try!(self.sstore.import_wallet(json, password));
		Ok(Address::from(address).into())
	}

	/// Returns addresses of all accounts.
	pub fn accounts(&self) -> Result<Vec<Address>, Error> {
		let accounts = try!(self.sstore.accounts());
		Ok(accounts)
	}

	/// Gets addresses visile for dapp.
	pub fn dapps_addresses(&self, dapp: DappId) -> Result<Vec<Address>, Error> {
		let accounts = self.dapps_settings.read().get();
		Ok(accounts.get(&dapp).map(|settings| settings.accounts.clone()).unwrap_or_else(Vec::new))
	}

	/// Sets addresses visile for dapp.
	pub fn set_dapps_addresses(&self, dapp: DappId, addresses: Vec<Address>) -> Result<(), Error> {
		self.dapps_settings.write().set_accounts(dapp, addresses);
		Ok(())
	}

	/// Returns each address along with metadata.
	pub fn addresses_info(&self) -> Result<HashMap<Address, AccountMeta>, Error> {
		Ok(self.address_book.read().get())
	}

	/// Returns each address along with metadata.
	pub fn set_address_name(&self, account: Address, name: String) -> Result<(), Error> {
		Ok(self.address_book.write().set_name(account, name))
	}

	/// Returns each address along with metadata.
	pub fn set_address_meta(&self, account: Address, meta: String) -> Result<(), Error> {
		Ok(self.address_book.write().set_meta(account, meta))
	}

	/// Removes and address from the addressbook
	pub fn remove_address(&self, addr: Address) -> Result<(), Error> {
		Ok(self.address_book.write().remove(addr))
	}

	/// Returns each account along with name and meta.
	pub fn accounts_info(&self) -> Result<HashMap<Address, AccountMeta>, Error> {
		let r: HashMap<Address, AccountMeta> = try!(self.sstore.accounts())
			.into_iter()
			.map(|a| (a.clone(), self.account_meta(a).ok().unwrap_or_default()))
			.collect();
		Ok(r)
	}

	/// Returns each account along with name and meta.
	pub fn account_meta(&self, account: Address) -> Result<AccountMeta, Error> {
		Ok(AccountMeta {
			name: try!(self.sstore.name(&account)),
			meta: try!(self.sstore.meta(&account)),
			uuid: self.sstore.uuid(&account).ok().map(Into::into),	// allowed to not have a Uuid
		})
	}

	/// Returns each account along with name and meta.
	pub fn set_account_name(&self, account: Address, name: String) -> Result<(), Error> {
		try!(self.sstore.set_name(&account, name));
		Ok(())
	}

	/// Returns each account along with name and meta.
	pub fn set_account_meta(&self, account: Address, meta: String) -> Result<(), Error> {
		try!(self.sstore.set_meta(&account, meta));
		Ok(())
	}

	/// Returns `true` if the password for `account` is `password`. `false` if not.
	pub fn test_password(&self, account: &Address, password: &str) -> Result<bool, Error> {
		match self.sstore.sign(account, password, &Default::default()) {
			Ok(_) => Ok(true),
			Err(SSError::InvalidPassword) => Ok(false),
			Err(e) => Err(Error::SStore(e)),
		}
	}

	/// Permanently removes an account.
	pub fn kill_account(&self, account: &Address, password: &str) -> Result<(), Error> {
		try!(self.sstore.remove_account(account, &password));
		Ok(())
	}

	/// Changes the password of `account` from `password` to `new_password`. Fails if incorrect `password` given.
	pub fn change_password(&self, account: &Address, password: String, new_password: String) -> Result<(), Error> {
		self.sstore.change_password(account, &password, &new_password).map_err(Error::SStore)
	}

	/// Helper method used for unlocking accounts.
	fn unlock_account(&self, account: Address, password: String, unlock: Unlock) -> Result<(), Error> {
		// verify password by signing dump message
		// result may be discarded
		let _ = try!(self.sstore.sign(&account, &password, &Default::default()));

		// check if account is already unlocked pernamently, if it is, do nothing
		let mut unlocked = self.unlocked.lock();
		if let Some(data) = unlocked.get(&account) {
			if let Unlock::Perm = data.unlock {
				return Ok(())
			}
		}

		let data = AccountData {
			unlock: unlock,
			password: password,
		};

		unlocked.insert(account, data);
		Ok(())
	}

	fn password(&self, account: &Address) -> Result<String, Error> {
		let mut unlocked = self.unlocked.lock();
		let data = try!(unlocked.get(account).ok_or(Error::NotUnlocked)).clone();
		if let Unlock::Temp = data.unlock {
			unlocked.remove(account).expect("data exists: so key must exist: qed");
		}
		if let Unlock::Timed(ref end) = data.unlock {
			if Instant::now() > *end {
				unlocked.remove(account).expect("data exists: so key must exist: qed");
				return Err(Error::NotUnlocked);
			}
		}
		Ok(data.password.clone())
	}

	/// Unlocks account permanently.
	pub fn unlock_account_permanently(&self, account: Address, password: String) -> Result<(), Error> {
		self.unlock_account(account, password, Unlock::Perm)
	}

	/// Unlocks account temporarily (for one signing).
	pub fn unlock_account_temporarily(&self, account: Address, password: String) -> Result<(), Error> {
		self.unlock_account(account, password, Unlock::Temp)
	}

	/// Unlocks account temporarily with a timeout.
	pub fn unlock_account_timed(&self, account: Address, password: String, duration_ms: u32) -> Result<(), Error> {
		self.unlock_account(account, password, Unlock::Timed(Instant::now() + Duration::from_millis(duration_ms as u64)))
	}

	/// Checks if given account is unlocked
	pub fn is_unlocked(&self, account: Address) -> bool {
		let unlocked = self.unlocked.lock();
		unlocked.get(&account).is_some()
	}

	/// Signs the message. If password is not provided the account must be unlocked.
	pub fn sign(&self, account: Address, password: Option<String>, message: Message) -> Result<Signature, Error> {
		let password = try!(password.map(Ok).unwrap_or_else(|| self.password(&account)));
		Ok(try!(self.sstore.sign(&account, &password, &message)))
	}

	/// Decrypts a message. If password is not provided the account must be unlocked.
	pub fn decrypt(&self, account: Address, password: Option<String>, shared_mac: &[u8], message: &[u8]) -> Result<Vec<u8>, Error> {
		let password = try!(password.map(Ok).unwrap_or_else(|| self.password(&account)));
		Ok(try!(self.sstore.decrypt(&account, &password, shared_mac, message)))
	}

	/// Returns the underlying `SecretStore` reference if one exists.
	pub fn list_geth_accounts(&self, testnet: bool) -> Vec<Address> {
		self.sstore.list_geth_accounts(testnet).into_iter().map(|a| Address::from(a).into()).collect()
	}

	/// Returns the underlying `SecretStore` reference if one exists.
	pub fn import_geth_accounts(&self, desired: Vec<Address>, testnet: bool) -> Result<Vec<Address>, Error> {
		self.sstore.import_geth_accounts(desired, testnet).map_err(Into::into)
	}
}

#[cfg(test)]
mod tests {
	use super::{AccountProvider, Unlock};
	use std::time::Instant;
	use ethstore::ethkey::{Generator, Random};

	#[test]
	fn unlock_account_temp() {
		let kp = Random.generate().unwrap();
		let ap = AccountProvider::transient_provider();
		assert!(ap.insert_account(kp.secret().clone(), "test").is_ok());
		assert!(ap.unlock_account_temporarily(kp.address(), "test1".into()).is_err());
		assert!(ap.unlock_account_temporarily(kp.address(), "test".into()).is_ok());
		assert!(ap.sign(kp.address(), None, Default::default()).is_ok());
		assert!(ap.sign(kp.address(), None, Default::default()).is_err());
	}

	#[test]
	fn unlock_account_perm() {
		let kp = Random.generate().unwrap();
		let ap = AccountProvider::transient_provider();
		assert!(ap.insert_account(kp.secret().clone(), "test").is_ok());
		assert!(ap.unlock_account_permanently(kp.address(), "test1".into()).is_err());
		assert!(ap.unlock_account_permanently(kp.address(), "test".into()).is_ok());
		assert!(ap.sign(kp.address(), None, Default::default()).is_ok());
		assert!(ap.sign(kp.address(), None, Default::default()).is_ok());
		assert!(ap.unlock_account_temporarily(kp.address(), "test".into()).is_ok());
		assert!(ap.sign(kp.address(), None, Default::default()).is_ok());
		assert!(ap.sign(kp.address(), None, Default::default()).is_ok());
	}

	#[test]
	fn unlock_account_timer() {
		let kp = Random.generate().unwrap();
		let ap = AccountProvider::transient_provider();
		assert!(ap.insert_account(kp.secret().clone(), "test").is_ok());
		assert!(ap.unlock_account_timed(kp.address(), "test1".into(), 60000).is_err());
		assert!(ap.unlock_account_timed(kp.address(), "test".into(), 60000).is_ok());
		assert!(ap.sign(kp.address(), None, Default::default()).is_ok());
		ap.unlocked.lock().get_mut(&kp.address()).unwrap().unlock = Unlock::Timed(Instant::now());
		assert!(ap.sign(kp.address(), None, Default::default()).is_err());
	}

	#[test]
	fn should_set_dapps_addresses() {
		// given
		let ap = AccountProvider::transient_provider();
		let app = "app1".to_owned();

		// when
		ap.set_dapps_addresses(app.clone(), vec![1.into(), 2.into()]).unwrap();

		// then
		assert_eq!(ap.dapps_addresses(app.clone()).unwrap(), vec![1.into(), 2.into()]);
	}
}
