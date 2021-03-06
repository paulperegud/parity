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

use ethcore::ethstore::{PresaleWallet, EthStore};
use ethcore::ethstore::dir::DiskDirectory;
use ethcore::account_provider::AccountProvider;
use helpers::{password_prompt, password_from_file};

#[derive(Debug, PartialEq)]
pub struct ImportWallet {
	pub iterations: u32,
	pub path: String,
	pub wallet_path: String,
	pub password_file: Option<String>,
}

pub fn execute(cmd: ImportWallet) -> Result<String, String> {
	let password: String = match cmd.password_file {
		Some(file) => try!(password_from_file(file)),
		None => try!(password_prompt()),
	};

	let dir = Box::new(DiskDirectory::create(cmd.path).unwrap());
	let secret_store = Box::new(EthStore::open_with_iterations(dir, cmd.iterations).unwrap());
	let acc_provider = AccountProvider::new(secret_store);
	let wallet = try!(PresaleWallet::open(cmd.wallet_path).map_err(|_| "Unable to open presale wallet."));
	let kp = try!(wallet.decrypt(&password).map_err(|_| "Invalid password."));
	let address = acc_provider.insert_account(*kp.secret(), &password).unwrap();
	Ok(format!("{:?}", address))
}
