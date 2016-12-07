// Copyright 2015, 2016 Ethcore (UK) Ltd.
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
use std::collections::{HashSet, HashMap, BTreeMap, VecDeque};
use std::sync::{Arc, Weak};
use std::path::{Path};
use std::fmt;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering as AtomicOrdering};
use std::time::{Instant};
use time::precise_time_ns;

// util
use util::{Bytes, PerfTimer, Itertools, Mutex, RwLock};
use util::{journaldb, TrieFactory, Trie};
use util::trie::TrieSpec;
use util::{U256, H256, Address, H2048, Uint, FixedHash};
use util::kvdb::*;

// other
use io::*;
use views::{HeaderView, BodyView, BlockView};
use error::{ImportError, ExecutionError, CallError, BlockError, ImportResult, Error as EthcoreError};
use header::BlockNumber;
use state::{State, CleanupMode};
use spec::Spec;
use basic_types::Seal;
use engines::Engine;
use service::ClientIoMessage;
use env_info::LastHashes;
use verification;
use verification::{PreverifiedBlock, Verifier};
use block::*;
use transaction::{LocalizedTransaction, SignedTransaction, Action};
use blockchain::extras::TransactionAddress;
use types::filter::Filter;
use types::mode::Mode as IpcMode;
use log_entry::LocalizedLogEntry;
use verification::queue::BlockQueue;
use blockchain::{BlockChain, BlockProvider, TreeRoute, ImportRoute};
use client::{
	BlockID, TransactionID, UncleID, TraceId, ClientConfig, BlockChainClient,
	MiningBlockChainClient, TraceFilter, CallAnalytics, BlockImportError, Mode,
	ChainNotify,
};
use client::Error as ClientError;
use env_info::EnvInfo;
use executive::{Executive, Executed, TransactOptions, contract_address};
use receipt::LocalizedReceipt;
use trace::{TraceDB, ImportRequest as TraceImportRequest, LocalizedTrace, Database as TraceDatabase};
use trace;
use trace::FlatTransactionTraces;
use evm::{Factory as EvmFactory, Schedule};
use miner::{Miner, MinerService};
use snapshot::{self, io as snapshot_io};
use factory::Factories;
use rlp::{decode, View, UntrustedRlp};
use state_db::StateDB;
use rand::OsRng;

// re-export
pub use types::blockchain_info::BlockChainInfo;
pub use types::block_status::BlockStatus;
pub use blockchain::CacheSize as BlockChainCacheSize;
pub use verification::queue::QueueInfo as BlockQueueInfo;

const MAX_TX_QUEUE_SIZE: usize = 4096;
const MAX_QUEUE_SIZE_TO_SLEEP_ON: usize = 2;
const MIN_HISTORY_SIZE: u64 = 8;

impl fmt::Display for BlockChainInfo {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "#{}.{}", self.best_block_number, self.best_block_hash)
	}
}

/// Report on the status of a client.
#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct ClientReport {
	/// How many blocks have been imported so far.
	pub blocks_imported: usize,
	/// How many transactions have been applied so far.
	pub transactions_applied: usize,
	/// How much gas has been processed so far.
	pub gas_processed: U256,
	/// Memory used by state DB
	pub state_db_mem: usize,
}

impl ClientReport {
	/// Alter internal reporting to reflect the additional `block` has been processed.
	pub fn accrue_block(&mut self, block: &PreverifiedBlock) {
		self.blocks_imported += 1;
		self.transactions_applied += block.transactions.len();
		self.gas_processed = self.gas_processed + block.header.gas_used().clone();
	}
}

struct SleepState {
	last_activity: Option<Instant>,
	last_autosleep: Option<Instant>,
}

impl SleepState {
	fn new(awake: bool) -> Self {
		SleepState {
			last_activity: match awake { false => None, true => Some(Instant::now()) },
			last_autosleep: match awake { false => Some(Instant::now()), true => None },
		}
	}
}

/// Blockchain database client backed by a persistent database. Owns and manages a blockchain and a block queue.
/// Call `import_block()` to import a block asynchronously; `flush_queue()` flushes the queue.
pub struct Client {
	mode: Mutex<Mode>,
	chain: RwLock<Arc<BlockChain>>,
	tracedb: RwLock<TraceDB<BlockChain>>,
	engine: Arc<Engine>,
	config: ClientConfig,
	pruning: journaldb::Algorithm,
	db: RwLock<Arc<Database>>,
	state_db: Mutex<StateDB>,
	block_queue: BlockQueue,
	report: RwLock<ClientReport>,
	import_lock: Mutex<()>,
	panic_handler: Arc<PanicHandler>,
	verifier: Box<Verifier>,
	miner: Arc<Miner>,
	sleep_state: Mutex<SleepState>,
	liveness: AtomicBool,
	io_channel: Mutex<IoChannel<ClientIoMessage>>,
	notify: RwLock<Vec<Weak<ChainNotify>>>,
	queue_transactions: AtomicUsize,
	last_hashes: RwLock<VecDeque<H256>>,
	factories: Factories,
	history: u64,
	rng: Mutex<OsRng>,
	on_mode_change: Mutex<Option<Box<FnMut(&Mode) + 'static + Send>>>,
}

impl Client {
	/// Create a new client with given spec and DB path and custom verifier.
	pub fn new(
		config: ClientConfig,
		spec: &Spec,
		path: &Path,
		miner: Arc<Miner>,
		message_channel: IoChannel<ClientIoMessage>,
		db_config: &DatabaseConfig,
	) -> Result<Arc<Client>, ClientError> {
		let path = path.to_path_buf();
		let gb = spec.genesis_block();

		let db = Arc::new(try!(Database::open(&db_config, &path.to_str().expect("DB path could not be converted to string.")).map_err(ClientError::Database)));
		let chain = Arc::new(BlockChain::new(config.blockchain.clone(), &gb, db.clone()));
		let tracedb = RwLock::new(TraceDB::new(config.tracing.clone(), db.clone(), chain.clone()));

		let trie_spec = match config.fat_db {
			true => TrieSpec::Fat,
			false => TrieSpec::Secure,
		};

		let journal_db = journaldb::new(db.clone(), config.pruning, ::db::COL_STATE);
		let mut state_db = StateDB::new(journal_db, config.state_cache_size);
		if state_db.journal_db().is_empty() && try!(spec.ensure_db_good(&mut state_db)) {
			let mut batch = DBTransaction::new(&db);
			try!(state_db.journal_under(&mut batch, 0, &spec.genesis_header().hash()));
			try!(db.write(batch).map_err(ClientError::Database));
		}

		trace!("Cleanup journal: DB Earliest = {:?}, Latest = {:?}", state_db.journal_db().earliest_era(), state_db.journal_db().latest_era());

		let history = if config.history < MIN_HISTORY_SIZE {
			info!(target: "client", "Ignoring pruning history parameter of {}\
				, falling back to minimum of {}",
				config.history, MIN_HISTORY_SIZE);
			MIN_HISTORY_SIZE
		} else {
			config.history
		};

		if let (Some(earliest), Some(latest)) = (state_db.journal_db().earliest_era(), state_db.journal_db().latest_era()) {
			if latest > earliest && latest - earliest > history {
				for era in earliest..(latest - history + 1) {
					trace!("Removing era {}", era);
					let mut batch = DBTransaction::new(&db);
					try!(state_db.mark_canonical(&mut batch, era, &chain.block_hash(era).expect("Old block not found in the database")));
					try!(db.write(batch).map_err(ClientError::Database));
				}
			}
		}

		if !chain.block_header(&chain.best_block_hash()).map_or(true, |h| state_db.journal_db().contains(h.state_root())) {
			warn!("State root not found for block #{} ({})", chain.best_block_number(), chain.best_block_hash().hex());
		}

		let engine = spec.engine.clone();

		let block_queue = BlockQueue::new(config.queue.clone(), engine.clone(), message_channel.clone(), config.verifier_type.verifying_seal());
		let panic_handler = PanicHandler::new_in_arc();
		panic_handler.forward_from(&block_queue);

		let awake = match config.mode { Mode::Dark(..) | Mode::Off => false, _ => true };

		let factories = Factories {
			vm: EvmFactory::new(config.vm_type.clone(), config.jump_table_size),
			trie: TrieFactory::new(trie_spec),
			accountdb: Default::default(),
		};

		let client = Client {
			sleep_state: Mutex::new(SleepState::new(awake)),
			liveness: AtomicBool::new(awake),
			mode: Mutex::new(config.mode.clone()),
			chain: RwLock::new(chain),
			tracedb: tracedb,
			engine: engine,
			pruning: config.pruning.clone(),
			verifier: verification::new(config.verifier_type.clone()),
			config: config,
			db: RwLock::new(db),
			state_db: Mutex::new(state_db),
			block_queue: block_queue,
			report: RwLock::new(Default::default()),
			import_lock: Mutex::new(()),
			panic_handler: panic_handler,
			miner: miner,
			io_channel: Mutex::new(message_channel),
			notify: RwLock::new(Vec::new()),
			queue_transactions: AtomicUsize::new(0),
			last_hashes: RwLock::new(VecDeque::new()),
			factories: factories,
			history: history,
			rng: Mutex::new(try!(OsRng::new().map_err(::util::UtilError::StdIo))),
			on_mode_change: Mutex::new(None),
		};
		Ok(Arc::new(client))
	}

	/// Adds an actor to be notified on certain events
	pub fn add_notify(&self, target: Arc<ChainNotify>) {
		self.notify.write().push(Arc::downgrade(&target));
	}

	fn notify<F>(&self, f: F) where F: Fn(&ChainNotify) {
		for np in self.notify.read().iter() {
			if let Some(n) = np.upgrade() {
				f(&*n);
			}
		}
	}

	/// Register an action to be done if a mode change happens. 
	pub fn on_mode_change<F>(&self, f: F) where F: 'static + FnMut(&Mode) + Send {
		*self.on_mode_change.lock() = Some(Box::new(f));
	}

	/// Flush the block import queue.
	pub fn flush_queue(&self) {
		self.block_queue.flush();
		while !self.block_queue.queue_info().is_empty() {
			self.import_verified_blocks();
		}
	}

	/// The env info as of the best block.
	fn latest_env_info(&self) -> EnvInfo {
		let header_data = self.best_block_header();
		let view = HeaderView::new(&header_data);

		EnvInfo {
			number: view.number(),
			author: view.author(),
			timestamp: view.timestamp(),
			difficulty: view.difficulty(),
			last_hashes: self.build_last_hashes(view.hash()),
			gas_used: U256::default(),
			gas_limit: view.gas_limit(),
		}
	}

	fn build_last_hashes(&self, parent_hash: H256) -> Arc<LastHashes> {
		{
			let hashes = self.last_hashes.read();
			if hashes.front().map_or(false, |h| h == &parent_hash) {
				let mut res = Vec::from(hashes.clone());
				res.resize(256, H256::default());
				return Arc::new(res);
			}
		}
		let mut last_hashes = LastHashes::new();
		last_hashes.resize(256, H256::default());
		last_hashes[0] = parent_hash;
		let chain = self.chain.read();
		for i in 0..255 {
			match chain.block_details(&last_hashes[i]) {
				Some(details) => {
					last_hashes[i + 1] = details.parent.clone();
				},
				None => break,
			}
		}
		let mut cached_hashes = self.last_hashes.write();
		*cached_hashes = VecDeque::from(last_hashes.clone());
		Arc::new(last_hashes)
	}

	fn check_and_close_block(&self, block: &PreverifiedBlock) -> Result<LockedBlock, ()> {
		let engine = &*self.engine;
		let header = &block.header;

		let chain = self.chain.read();
		// Check the block isn't so old we won't be able to enact it.
		let best_block_number = chain.best_block_number();
		if best_block_number >= self.history && header.number() <= best_block_number - self.history {
			warn!(target: "client", "Block import failed for #{} ({})\nBlock is ancient (current best block: #{}).", header.number(), header.hash(), best_block_number);
			return Err(());
		}

		// Verify Block Family
		let verify_family_result = self.verifier.verify_block_family(header, &block.bytes, engine, &**chain);
		if let Err(e) = verify_family_result {
			warn!(target: "client", "Stage 3 block verification failed for #{} ({})\nError: {:?}", header.number(), header.hash(), e);
			return Err(());
		};

		// Check if Parent is in chain
		let chain_has_parent = chain.block_header(header.parent_hash());
		if let Some(parent) = chain_has_parent {
			// Enact Verified Block
			let last_hashes = self.build_last_hashes(header.parent_hash().clone());
			let db = self.state_db.lock().boxed_clone_canon(header.parent_hash());

			let enact_result = enact_verified(block, engine, self.tracedb.read().tracing_enabled(), db, &parent, last_hashes, self.factories.clone());
			let locked_block = try!(enact_result.map_err(|e| {
				warn!(target: "client", "Block import failed for #{} ({})\nError: {:?}", header.number(), header.hash(), e);
			}));

			// Final Verification
			if let Err(e) = self.verifier.verify_block_final(header, locked_block.block().header()) {
				warn!(target: "client", "Stage 4 block verification failed for #{} ({})\nError: {:?}", header.number(), header.hash(), e);
				return Err(());
			}

			Ok(locked_block)
		} else {
			warn!(target: "client", "Block import failed for #{} ({}): Parent not found ({}) ", header.number(), header.hash(), header.parent_hash());
			Err(())
		}
	}

	fn calculate_enacted_retracted(&self, import_results: &[ImportRoute]) -> (Vec<H256>, Vec<H256>) {
		fn map_to_vec(map: Vec<(H256, bool)>) -> Vec<H256> {
			map.into_iter().map(|(k, _v)| k).collect()
		}

		// In ImportRoute we get all the blocks that have been enacted and retracted by single insert.
		// Because we are doing multiple inserts some of the blocks that were enacted in import `k`
		// could be retracted in import `k+1`. This is why to understand if after all inserts
		// the block is enacted or retracted we iterate over all routes and at the end final state
		// will be in the hashmap
		let map = import_results.iter().fold(HashMap::new(), |mut map, route| {
			for hash in &route.enacted {
				map.insert(hash.clone(), true);
			}
			for hash in &route.retracted {
				map.insert(hash.clone(), false);
			}
			map
		});

		// Split to enacted retracted (using hashmap value)
		let (enacted, retracted) = map.into_iter().partition(|&(_k, v)| v);
		// And convert tuples to keys
		(map_to_vec(enacted), map_to_vec(retracted))
	}

	/// This is triggered by a message coming from a block queue when the block is ready for insertion
	pub fn import_verified_blocks(&self) -> usize {
		let max_blocks_to_import = 4;
		let (imported_blocks, import_results, invalid_blocks, imported, duration, is_empty) = {
			let mut imported_blocks = Vec::with_capacity(max_blocks_to_import);
			let mut invalid_blocks = HashSet::new();
			let mut import_results = Vec::with_capacity(max_blocks_to_import);

			let _import_lock = self.import_lock.lock();
			let blocks = self.block_queue.drain(max_blocks_to_import);
			if blocks.is_empty() {
				return 0;
			}
			let _timer = PerfTimer::new("import_verified_blocks");
			let start = precise_time_ns();

			for block in blocks {
				let header = &block.header;
				let is_invalid = invalid_blocks.contains(header.parent_hash());
				if is_invalid {
					invalid_blocks.insert(header.hash());
					continue;
				}
				if let Ok(closed_block) = self.check_and_close_block(&block) {
					imported_blocks.push(header.hash());

					let route = self.commit_block(closed_block, &header.hash(), &block.bytes);
					import_results.push(route);

					self.report.write().accrue_block(&block);
				} else {
					invalid_blocks.insert(header.hash());
				}
			}

			let imported = imported_blocks.len();
			let invalid_blocks = invalid_blocks.into_iter().collect::<Vec<H256>>();

			if !invalid_blocks.is_empty() {
				self.block_queue.mark_as_bad(&invalid_blocks);
			}
			let is_empty = self.block_queue.mark_as_good(&imported_blocks);
			let duration_ns = precise_time_ns() - start;
			(imported_blocks, import_results, invalid_blocks, imported, duration_ns, is_empty)
		};

		{
			if !imported_blocks.is_empty() && is_empty {
				let (enacted, retracted) = self.calculate_enacted_retracted(&import_results);

				if is_empty {
					self.miner.chain_new_blocks(self, &imported_blocks, &invalid_blocks, &enacted, &retracted);
				}

				self.notify(|notify| {
					notify.new_blocks(
						imported_blocks.clone(),
						invalid_blocks.clone(),
						enacted.clone(),
						retracted.clone(),
						Vec::new(),
						duration,
					);
				});
			}
		}

		self.db.read().flush().expect("DB flush failed.");
		imported
	}

	/// Import a block with transaction receipts.
	/// The block is guaranteed to be the next best blocks in the first block sequence.
	/// Does no sealing or transaction validation.
	fn import_old_block(&self, block_bytes: Bytes, receipts_bytes: Bytes) -> Result<H256, ::error::Error> {
		let block = BlockView::new(&block_bytes);
		let header = block.header();
		let hash = header.hash();
		let _import_lock = self.import_lock.lock();
		{
			let _timer = PerfTimer::new("import_old_block");
			let mut rng = self.rng.lock();
			let chain = self.chain.read();

			// verify block.
			try!(::snapshot::verify_old_block(
				&mut *rng,
				&header,
				&*self.engine,
				&*chain,
				Some(&block_bytes),
				false,
			));

			// Commit results
			let receipts = ::rlp::decode(&receipts_bytes);
			let mut batch = DBTransaction::new(&self.db.read());
			chain.insert_unordered_block(&mut batch, &block_bytes, receipts, None, false, true);
			// Final commit to the DB
			self.db.read().write_buffered(batch);
			chain.commit();
		}
		self.db.read().flush().expect("DB flush failed.");
		Ok(hash)
	}

	fn commit_block<B>(&self, block: B, hash: &H256, block_data: &[u8]) -> ImportRoute where B: IsBlock + Drain {
		let number = block.header().number();
		let parent = block.header().parent_hash().clone();
		let chain = self.chain.read();

		// Commit results
		let receipts = block.receipts().to_owned();
		let traces = block.traces().clone().unwrap_or_else(Vec::new);
		let traces: Vec<FlatTransactionTraces> = traces.into_iter()
			.map(Into::into)
			.collect();

		//let traces = From::from(block.traces().clone().unwrap_or_else(Vec::new));

		let mut batch = DBTransaction::new(&self.db.read());
		// CHECK! I *think* this is fine, even if the state_root is equal to another
		// already-imported block of the same number.
		// TODO: Prove it with a test.
		let mut state = block.drain();

		state.journal_under(&mut batch, number, hash).expect("DB commit failed");

		if number >= self.history {
			let n = number - self.history;
			if let Some(ancient_hash) = chain.block_hash(n) {
				state.mark_canonical(&mut batch, n, &ancient_hash).expect("DB commit failed");
			} else {
				debug!(target: "client", "Missing expected hash for block {}", n);
			}
		}

		let route = chain.insert_block(&mut batch, block_data, receipts);
		self.tracedb.read().import(&mut batch, TraceImportRequest {
			traces: traces.into(),
			block_hash: hash.clone(),
			block_number: number,
			enacted: route.enacted.clone(),
			retracted: route.retracted.len()
		});

		let is_canon = route.enacted.last().map_or(false, |h| h == hash);
		state.sync_cache(&route.enacted, &route.retracted, is_canon);
		// Final commit to the DB
		self.db.read().write_buffered(batch);
		chain.commit();
		self.update_last_hashes(&parent, hash);
		route
	}

	fn update_last_hashes(&self, parent: &H256, hash: &H256) {
		let mut hashes = self.last_hashes.write();
		if hashes.front().map_or(false, |h| h == parent) {
			if hashes.len() > 255 {
				hashes.pop_back();
			}
			hashes.push_front(hash.clone());
		}
	}

	/// Import transactions from the IO queue
	pub fn import_queued_transactions(&self, transactions: &[Bytes]) -> usize {
		let _timer = PerfTimer::new("import_queued_transactions");
		self.queue_transactions.fetch_sub(transactions.len(), AtomicOrdering::SeqCst);
		let txs = transactions.iter().filter_map(|bytes| UntrustedRlp::new(bytes).as_val().ok()).collect();
		let results = self.miner.import_external_transactions(self, txs);
		results.len()
	}

	/// Attempt to get a copy of a specific block's final state.
	///
	/// This will not fail if given BlockID::Latest.
	/// Otherwise, this can fail (but may not) if the DB prunes state.
	pub fn state_at(&self, id: BlockID) -> Option<State> {
		// fast path for latest state.
		match id.clone() {
			BlockID::Pending => return self.miner.pending_state().or_else(|| Some(self.state())),
			BlockID::Latest => return Some(self.state()),
			_ => {},
		}

		let block_number = match self.block_number(id.clone()) {
			Some(num) => num,
			None => return None,
		};

		self.block_header(id).and_then(|header| {
			let db = self.state_db.lock().boxed_clone();

			// early exit for pruned blocks
			if db.is_pruned() && self.chain.read().best_block_number() >= block_number + self.history {
				return None;
			}

			let root = HeaderView::new(&header).state_root();

			State::from_existing(db, root, self.engine.account_start_nonce(), self.factories.clone()).ok()
		})
	}

	/// Attempt to get a copy of a specific block's beginning state.
	///
	/// This will not fail if given BlockID::Latest.
	/// Otherwise, this can fail (but may not) if the DB prunes state.
	pub fn state_at_beginning(&self, id: BlockID) -> Option<State> {
		// fast path for latest state.
		match id {
			BlockID::Pending => self.state_at(BlockID::Latest),
			id => match self.block_number(id) {
				None | Some(0) => None,
				Some(n) => self.state_at(BlockID::Number(n - 1)),
			}
		}
	}

	/// Get a copy of the best block's state.
	pub fn state(&self) -> State {
		let header = self.best_block_header();
		let header = HeaderView::new(&header);
		State::from_existing(
			self.state_db.lock().boxed_clone_canon(&header.hash()),
			header.state_root(),
			self.engine.account_start_nonce(),
			self.factories.clone())
		.expect("State root of best block header always valid.")
	}

	/// Get info on the cache.
	pub fn blockchain_cache_info(&self) -> BlockChainCacheSize {
		self.chain.read().cache_size()
	}

	/// Get the report.
	pub fn report(&self) -> ClientReport {
		let mut report = self.report.read().clone();
		report.state_db_mem = self.state_db.lock().mem_used();
		report
	}

	/// Tick the client.
	// TODO: manage by real events.
	pub fn tick(&self) {
		self.chain.read().collect_garbage();
		self.block_queue.collect_garbage();
		self.tracedb.read().collect_garbage();

		let mode = self.mode.lock().clone();
		match mode {
			Mode::Dark(timeout) => {
				let mut ss = self.sleep_state.lock();
				if let Some(t) = ss.last_activity {
					if Instant::now() > t + timeout {
						self.sleep();
						ss.last_activity = None;
					}
				}
			}
			Mode::Passive(timeout, wakeup_after) => {
				let mut ss = self.sleep_state.lock();
				let now = Instant::now();
				if let Some(t) = ss.last_activity {
					if now > t + timeout {
						self.sleep();
						ss.last_activity = None;
						ss.last_autosleep = Some(now);
					}
				}
				if let Some(t) = ss.last_autosleep {
					if now > t + wakeup_after {
						self.wake_up();
						ss.last_activity = Some(now);
						ss.last_autosleep = None;
					}
				}
			}
			_ => {}
		}
	}

	/// Look up the block number for the given block ID.
	pub fn block_number(&self, id: BlockID) -> Option<BlockNumber> {
		match id {
			BlockID::Number(number) => Some(number),
			BlockID::Hash(ref hash) => self.chain.read().block_number(hash),
			BlockID::Earliest => Some(0),
			BlockID::Latest | BlockID::Pending => Some(self.chain.read().best_block_number()),
		}
	}

	/// Take a snapshot at the given block.
	/// If the ID given is "latest", this will default to 1000 blocks behind.
	pub fn take_snapshot<W: snapshot_io::SnapshotWriter + Send>(&self, writer: W, at: BlockID, p: &snapshot::Progress) -> Result<(), EthcoreError> {
		let db = self.state_db.lock().journal_db().boxed_clone();
		let best_block_number = self.chain_info().best_block_number;
		let block_number = try!(self.block_number(at).ok_or(snapshot::Error::InvalidStartingBlock(at)));

		if best_block_number > self.history + block_number && db.is_pruned() {
			return Err(snapshot::Error::OldBlockPrunedDB.into());
		}

		let history = ::std::cmp::min(self.history, 1000);

		let start_hash = match at {
			BlockID::Latest => {
				let start_num = match db.earliest_era() {
					Some(era) => ::std::cmp::max(era, best_block_number - history),
					None => best_block_number - history,
				};

				match self.block_hash(BlockID::Number(start_num)) {
					Some(h) => h,
					None => return Err(snapshot::Error::InvalidStartingBlock(at).into()),
				}
			}
			_ => match self.block_hash(at) {
				Some(hash) => hash,
				None => return Err(snapshot::Error::InvalidStartingBlock(at).into()),
			},
		};

		try!(snapshot::take_snapshot(&self.chain.read(), start_hash, db.as_hashdb(), writer, p));

		Ok(())
	}

	/// Ask the client what the history parameter is.
	pub fn pruning_history(&self) -> u64 {
		self.history
	}

	fn block_hash(chain: &BlockChain, id: BlockID) -> Option<H256> {
		match id {
			BlockID::Hash(hash) => Some(hash),
			BlockID::Number(number) => chain.block_hash(number),
			BlockID::Earliest => chain.block_hash(0),
			BlockID::Latest | BlockID::Pending => Some(chain.best_block_hash()),
		}
	}

	fn transaction_address(&self, id: TransactionID) -> Option<TransactionAddress> {
		match id {
			TransactionID::Hash(ref hash) => self.chain.read().transaction_address(hash),
			TransactionID::Location(id, index) => Self::block_hash(&self.chain.read(), id).map(|hash| TransactionAddress {
				block_hash: hash,
				index: index,
			})
		}
	}

	fn wake_up(&self) {
		if !self.liveness.load(AtomicOrdering::Relaxed) {
			self.liveness.store(true, AtomicOrdering::Relaxed);
			self.notify(|n| n.start());
			trace!(target: "mode", "wake_up: Waking.");
		}
	}

	fn sleep(&self) {
		if self.liveness.load(AtomicOrdering::Relaxed) {
			// only sleep if the import queue is mostly empty.
			if self.queue_info().total_queue_size() <= MAX_QUEUE_SIZE_TO_SLEEP_ON {
				self.liveness.store(false, AtomicOrdering::Relaxed);
				self.notify(|n| n.stop());
				trace!(target: "mode", "sleep: Sleeping.");
			} else {
				trace!(target: "mode", "sleep: Cannot sleep - syncing ongoing.");
				// TODO: Consider uncommenting.
				//*self.last_activity.lock() = Some(Instant::now());
			}
		}
	}
}

impl snapshot::DatabaseRestore for Client {
	/// Restart the client with a new backend
	fn restore_db(&self, new_db: &str) -> Result<(), EthcoreError> {
		trace!(target: "snapshot", "Replacing client database with {:?}", new_db);

		let _import_lock = self.import_lock.lock();
		let mut state_db = self.state_db.lock();
		let mut chain = self.chain.write();
		let mut tracedb = self.tracedb.write();
		self.miner.clear();
		let db = self.db.write();
		try!(db.restore(new_db));

		let cache_size = state_db.cache_size();
		*state_db = StateDB::new(journaldb::new(db.clone(), self.pruning, ::db::COL_STATE), cache_size);
		*chain = Arc::new(BlockChain::new(self.config.blockchain.clone(), &[], db.clone()));
		*tracedb = TraceDB::new(self.config.tracing.clone(), db.clone(), chain.clone());
		Ok(())
	}
}


impl BlockChainClient for Client {
	fn call(&self, t: &SignedTransaction, block: BlockID, analytics: CallAnalytics) -> Result<Executed, CallError> {
		let header = try!(self.block_header(block).ok_or(CallError::StatePruned));
		let view = HeaderView::new(&header);
		let last_hashes = self.build_last_hashes(view.parent_hash());
		let env_info = EnvInfo {
			number: view.number(),
			author: view.author(),
			timestamp: view.timestamp(),
			difficulty: view.difficulty(),
			last_hashes: last_hashes,
			gas_used: U256::zero(),
			gas_limit: U256::max_value(),
		};
		// that's just a copy of the state.
		let mut state = try!(self.state_at(block).ok_or(CallError::StatePruned));
		let original_state = if analytics.state_diffing { Some(state.clone()) } else { None };

		let sender = try!(t.sender().map_err(|e| {
			let message = format!("Transaction malformed: {:?}", e);
			ExecutionError::TransactionMalformed(message)
		}));
		let balance = state.balance(&sender);
		let needed_balance = t.value + t.gas * t.gas_price;
		if balance < needed_balance {
			// give the sender a sufficient balance
			state.add_balance(&sender, &(needed_balance - balance), CleanupMode::NoEmpty);
		}
		let options = TransactOptions { tracing: analytics.transaction_tracing, vm_tracing: analytics.vm_tracing, check_nonce: false };
		let mut ret = try!(Executive::new(&mut state, &env_info, &*self.engine, &self.factories.vm).transact(t, options));

		// TODO gav move this into Executive.
		ret.state_diff = original_state.map(|original| state.diff_from(original));

		Ok(ret)
	}

	fn replay(&self, id: TransactionID, analytics: CallAnalytics) -> Result<Executed, CallError> {
		let address = try!(self.transaction_address(id).ok_or(CallError::TransactionNotFound));
		let header_data = try!(self.block_header(BlockID::Hash(address.block_hash)).ok_or(CallError::StatePruned));
		let body_data = try!(self.block_body(BlockID::Hash(address.block_hash)).ok_or(CallError::StatePruned));
		let mut state = try!(self.state_at_beginning(BlockID::Hash(address.block_hash)).ok_or(CallError::StatePruned));
		let txs = BodyView::new(&body_data).transactions();

		if address.index >= txs.len() {
			return Err(CallError::TransactionNotFound);
		}

		let options = TransactOptions { tracing: analytics.transaction_tracing, vm_tracing: analytics.vm_tracing, check_nonce: false };
		let view = HeaderView::new(&header_data);
		let last_hashes = self.build_last_hashes(view.hash());
		let mut env_info = EnvInfo {
			number: view.number(),
			author: view.author(),
			timestamp: view.timestamp(),
			difficulty: view.difficulty(),
			last_hashes: last_hashes,
			gas_used: U256::default(),
			gas_limit: view.gas_limit(),
		};
		for t in txs.iter().take(address.index) {
			match Executive::new(&mut state, &env_info, &*self.engine, &self.factories.vm).transact(t, Default::default()) {
				Ok(x) => { env_info.gas_used = env_info.gas_used + x.gas_used; }
				Err(ee) => { return Err(CallError::Execution(ee)) }
			}
		}
		let t = &txs[address.index];

		let original_state = if analytics.state_diffing { Some(state.clone()) } else { None };
		let mut ret = try!(Executive::new(&mut state, &env_info, &*self.engine, &self.factories.vm).transact(t, options));
		ret.state_diff = original_state.map(|original| state.diff_from(original));

		Ok(ret)
	}

	fn keep_alive(&self) {
		let should_wake = match &*self.mode.lock() {
			&Mode::Dark(..) | &Mode::Passive(..) => true,
			_ => false,
		};
		if should_wake {
			self.wake_up();
			(*self.sleep_state.lock()).last_activity = Some(Instant::now());
		}
	}

	fn mode(&self) -> IpcMode {
		let r = self.mode.lock().clone().into();
		trace!(target: "mode", "Asked for mode = {:?}. returning {:?}", &*self.mode.lock(), r);
		r
	}

	fn set_mode(&self, new_mode: IpcMode) {
		trace!(target: "mode", "Client::set_mode({:?})", new_mode);
		{
			let mut mode = self.mode.lock();
			*mode = new_mode.clone().into();
			trace!(target: "mode", "Mode now {:?}", &*mode);
			match *self.on_mode_change.lock() {
				Some(ref mut f) => {
					trace!(target: "mode", "Making callback...");
					f(&*mode)
				},
				_ => {} 
			}
		}
		match new_mode {
			IpcMode::Active => self.wake_up(),
			IpcMode::Off => self.sleep(),
			_ => {(*self.sleep_state.lock()).last_activity = Some(Instant::now()); }
		}
	}

	fn best_block_header(&self) -> Bytes {
		self.chain.read().best_block_header()
	}

	fn block_header(&self, id: BlockID) -> Option<Bytes> {
		let chain = self.chain.read();
		Self::block_hash(&chain, id).and_then(|hash| chain.block_header_data(&hash))
	}

	fn block_body(&self, id: BlockID) -> Option<Bytes> {
		let chain = self.chain.read();
		Self::block_hash(&chain, id).and_then(|hash| chain.block_body(&hash))
	}

	fn block(&self, id: BlockID) -> Option<Bytes> {
		if let BlockID::Pending = id {
			if let Some(block) = self.miner.pending_block() {
				return Some(block.rlp_bytes(Seal::Without));
			}
		}
		let chain = self.chain.read();
		Self::block_hash(&chain, id).and_then(|hash| {
			chain.block(&hash)
		})
	}

	fn block_status(&self, id: BlockID) -> BlockStatus {
		let chain = self.chain.read();
		match Self::block_hash(&chain, id) {
			Some(ref hash) if chain.is_known(hash) => BlockStatus::InChain,
			Some(hash) => self.block_queue.status(&hash).into(),
			None => BlockStatus::Unknown
		}
	}

	fn block_total_difficulty(&self, id: BlockID) -> Option<U256> {
		if let BlockID::Pending = id {
			if let Some(block) = self.miner.pending_block() {
				return Some(*block.header.difficulty() + self.block_total_difficulty(BlockID::Latest).expect("blocks in chain have details; qed"));
			}
		}
		let chain = self.chain.read();
		Self::block_hash(&chain, id).and_then(|hash| chain.block_details(&hash)).map(|d| d.total_difficulty)
	}

	fn nonce(&self, address: &Address, id: BlockID) -> Option<U256> {
		self.state_at(id).map(|s| s.nonce(address))
	}

	fn block_hash(&self, id: BlockID) -> Option<H256> {
		let chain = self.chain.read();
		Self::block_hash(&chain, id)
	}

	fn code(&self, address: &Address, id: BlockID) -> Option<Option<Bytes>> {
		self.state_at(id).map(|s| s.code(address).map(|c| (*c).clone()))
	}

	fn balance(&self, address: &Address, id: BlockID) -> Option<U256> {
		self.state_at(id).map(|s| s.balance(address))
	}

	fn storage_at(&self, address: &Address, position: &H256, id: BlockID) -> Option<H256> {
		self.state_at(id).map(|s| s.storage_at(address, position))
	}

	fn list_accounts(&self, id: BlockID) -> Option<Vec<Address>> {
		if !self.factories.trie.is_fat() {
			trace!(target: "fatdb", "list_accounts: Not a fat DB");
			return None;
		}

		let state = match self.state_at(id) {
			Some(state) => state,
			_ => return None,
		};

		let (root, db) = state.drop();
		let trie = match self.factories.trie.readonly(db.as_hashdb(), &root) {
			Ok(trie) => trie,
			_ => {
				trace!(target: "fatdb", "list_accounts: Couldn't open the DB");
				return None;
			}
		};

		let iter = match trie.iter() {
			Ok(iter) => iter,
			_ => return None,
		};

		let accounts = iter.filter_map(|item| {
			item.ok().map(|(addr, _)| Address::from_slice(&addr))
		}).collect();

		Some(accounts)
	}

	fn transaction(&self, id: TransactionID) -> Option<LocalizedTransaction> {
		self.transaction_address(id).and_then(|address| self.chain.read().transaction(&address))
	}

	fn transaction_block(&self, id: TransactionID) -> Option<H256> {
		self.transaction_address(id).map(|addr| addr.block_hash)
	}

	fn uncle(&self, id: UncleID) -> Option<Bytes> {
		let index = id.position;
		self.block_body(id.block).and_then(|body| BodyView::new(&body).uncle_rlp_at(index))
	}

	fn transaction_receipt(&self, id: TransactionID) -> Option<LocalizedReceipt> {
		let chain = self.chain.read();
		self.transaction_address(id)
			.and_then(|address| chain.block_number(&address.block_hash).and_then(|block_number| {
			let t = chain.block_body(&address.block_hash)
				.and_then(|block| {
					BodyView::new(&block).localized_transaction_at(&address.block_hash, block_number, address.index)
				});

			let tx_and_sender = t.and_then(|tx| tx.sender().ok().map(|sender| (tx, sender)));

			match (tx_and_sender, chain.transaction_receipt(&address)) {
				(Some((tx, sender)), Some(receipt)) => {
					let block_hash = tx.block_hash.clone();
					let block_number = tx.block_number.clone();
					let transaction_hash = tx.hash();
					let transaction_index = tx.transaction_index;
					let prior_gas_used = match tx.transaction_index {
						0 => U256::zero(),
						i => {
							let prior_address = TransactionAddress { block_hash: address.block_hash, index: i - 1 };
							let prior_receipt = chain.transaction_receipt(&prior_address).expect("Transaction receipt at `address` exists; `prior_address` has lower index in same block; qed");
							prior_receipt.gas_used
						}
					};
					Some(LocalizedReceipt {
						transaction_hash: tx.hash(),
						transaction_index: tx.transaction_index,
						block_hash: tx.block_hash,
						block_number: tx.block_number,
						cumulative_gas_used: receipt.gas_used,
						gas_used: receipt.gas_used - prior_gas_used,
						contract_address: match tx.action {
							Action::Call(_) => None,
							Action::Create => Some(contract_address(&sender, &tx.nonce))
						},
						logs: receipt.logs.into_iter().enumerate().map(|(i, log)| LocalizedLogEntry {
							entry: log,
							block_hash: block_hash.clone(),
							block_number: block_number,
							transaction_hash: transaction_hash.clone(),
							transaction_index: transaction_index,
							log_index: i
						}).collect(),
						log_bloom: receipt.log_bloom,
						state_root: receipt.state_root,
					})
				},
				_ => None
			}
		}))
	}

	fn tree_route(&self, from: &H256, to: &H256) -> Option<TreeRoute> {
		let chain = self.chain.read();
		match chain.is_known(from) && chain.is_known(to) {
			true => Some(chain.tree_route(from.clone(), to.clone())),
			false => None
		}
	}

	fn find_uncles(&self, hash: &H256) -> Option<Vec<H256>> {
		self.chain.read().find_uncle_hashes(hash, self.engine.maximum_uncle_age())
	}

	fn state_data(&self, hash: &H256) -> Option<Bytes> {
		self.state_db.lock().journal_db().state(hash)
	}

	fn block_receipts(&self, hash: &H256) -> Option<Bytes> {
		self.chain.read().block_receipts(hash).map(|receipts| ::rlp::encode(&receipts).to_vec())
	}

	fn import_block(&self, bytes: Bytes) -> Result<H256, BlockImportError> {
		use verification::queue::kind::HasHash;
		use verification::queue::kind::blocks::Unverified;

		// create unverified block here so the `sha3` calculation can be cached.
		let unverified = Unverified::new(bytes);

		{
			if self.chain.read().is_known(&unverified.hash()) {
				return Err(BlockImportError::Import(ImportError::AlreadyInChain));
			}
			if self.block_status(BlockID::Hash(unverified.parent_hash())) == BlockStatus::Unknown {
				return Err(BlockImportError::Block(BlockError::UnknownParent(unverified.parent_hash())));
			}
		}
		Ok(try!(self.block_queue.import(unverified)))
	}

	fn import_block_with_receipts(&self, block_bytes: Bytes, receipts_bytes: Bytes) -> Result<H256, BlockImportError> {
		{
			// check block order
			let header = BlockView::new(&block_bytes).header_view();
			if self.chain.read().is_known(&header.hash()) {
				return Err(BlockImportError::Import(ImportError::AlreadyInChain));
			}
			if self.block_status(BlockID::Hash(header.parent_hash())) == BlockStatus::Unknown {
				return Err(BlockImportError::Block(BlockError::UnknownParent(header.parent_hash())));
			}
		}
		self.import_old_block(block_bytes, receipts_bytes).map_err(Into::into)
	}

	fn queue_info(&self) -> BlockQueueInfo {
		self.block_queue.queue_info()
	}

	fn clear_queue(&self) {
		self.block_queue.clear();
	}

	fn chain_info(&self) -> BlockChainInfo {
		self.chain.read().chain_info()
	}

	fn additional_params(&self) -> BTreeMap<String, String> {
		self.engine.additional_params().into_iter().collect()
	}

	fn blocks_with_bloom(&self, bloom: &H2048, from_block: BlockID, to_block: BlockID) -> Option<Vec<BlockNumber>> {
		match (self.block_number(from_block), self.block_number(to_block)) {
			(Some(from), Some(to)) => Some(self.chain.read().blocks_with_bloom(bloom, from, to)),
			_ => None
		}
	}

	fn logs(&self, filter: Filter) -> Vec<LocalizedLogEntry> {
		let blocks = filter.bloom_possibilities().iter()
			.filter_map(|bloom| self.blocks_with_bloom(bloom, filter.from_block.clone(), filter.to_block.clone()))
			.flat_map(|m| m)
			// remove duplicate elements
			.collect::<HashSet<u64>>()
			.into_iter()
			.collect::<Vec<u64>>();

		self.chain.read().logs(blocks, |entry| filter.matches(entry), filter.limit)
	}

	fn filter_traces(&self, filter: TraceFilter) -> Option<Vec<LocalizedTrace>> {
		let start = self.block_number(filter.range.start);
		let end = self.block_number(filter.range.end);

		match (start, end) {
			(Some(s), Some(e)) => {
				let filter = trace::Filter {
					range: s as usize..e as usize,
					from_address: From::from(filter.from_address),
					to_address: From::from(filter.to_address),
				};

				let traces = self.tracedb.read().filter(&filter);
				Some(traces)
			},
			_ => None,
		}
	}

	fn trace(&self, trace: TraceId) -> Option<LocalizedTrace> {
		let trace_address = trace.address;
		self.transaction_address(trace.transaction)
			.and_then(|tx_address| {
				self.block_number(BlockID::Hash(tx_address.block_hash))
					.and_then(|number| self.tracedb.read().trace(number, tx_address.index, trace_address))
			})
	}

	fn transaction_traces(&self, transaction: TransactionID) -> Option<Vec<LocalizedTrace>> {
		self.transaction_address(transaction)
			.and_then(|tx_address| {
				self.block_number(BlockID::Hash(tx_address.block_hash))
					.and_then(|number| self.tracedb.read().transaction_traces(number, tx_address.index))
			})
	}

	fn block_traces(&self, block: BlockID) -> Option<Vec<LocalizedTrace>> {
		self.block_number(block)
			.and_then(|number| self.tracedb.read().block_traces(number))
	}

	fn last_hashes(&self) -> LastHashes {
		(*self.build_last_hashes(self.chain.read().best_block_hash())).clone()
	}

	fn queue_transactions(&self, transactions: Vec<Bytes>) {
		if self.queue_transactions.load(AtomicOrdering::Relaxed) > MAX_TX_QUEUE_SIZE {
			debug!("Ignoring {} transactions: queue is full", transactions.len());
		} else {
			let len = transactions.len();
			match self.io_channel.lock().send(ClientIoMessage::NewTransactions(transactions)) {
				Ok(_) => {
					self.queue_transactions.fetch_add(len, AtomicOrdering::SeqCst);
				}
				Err(e) => {
					debug!("Ignoring {} transactions: error queueing: {}", len, e);
				}
			}
		}
	}

	fn pending_transactions(&self) -> Vec<SignedTransaction> {
		self.miner.pending_transactions(self.chain.read().best_block_number())
	}

	fn signing_network_id(&self) -> Option<u8> {
		self.engine.signing_network_id(&self.latest_env_info())
	}

	fn block_extra_info(&self, id: BlockID) -> Option<BTreeMap<String, String>> {
		self.block_header(id)
			.map(|block| decode(&block))
			.map(|header| self.engine.extra_info(&header))
	}

	fn uncle_extra_info(&self, id: UncleID) -> Option<BTreeMap<String, String>> {
		self.uncle(id)
			.map(|header| self.engine.extra_info(&decode(&header)))
	}
}

impl MiningBlockChainClient for Client {

	fn latest_schedule(&self) -> Schedule {
		self.engine.schedule(&self.latest_env_info())
	}

	fn prepare_open_block(&self, author: Address, gas_range_target: (U256, U256), extra_data: Bytes) -> OpenBlock {
		let engine = &*self.engine;
		let chain = self.chain.read();
		let h = chain.best_block_hash();

		let mut open_block = OpenBlock::new(
			engine,
			self.factories.clone(),
			false,	// TODO: this will need to be parameterised once we want to do immediate mining insertion.
			self.state_db.lock().boxed_clone_canon(&h),
			&chain.block_header(&h).expect("h is best block hash: so its header must exist: qed"),
			self.build_last_hashes(h.clone()),
			author,
			gas_range_target,
			extra_data,
		).expect("OpenBlock::new only fails if parent state root invalid; state root of best block's header is never invalid; qed");

		// Add uncles
		chain
			.find_uncle_headers(&h, engine.maximum_uncle_age())
			.unwrap_or_else(Vec::new)
			.into_iter()
			.take(engine.maximum_uncle_count())
			.foreach(|h| {
				open_block.push_uncle(h).expect("pushing maximum_uncle_count;
												open_block was just created;
												push_uncle is not ok only if more than maximum_uncle_count is pushed;
												so all push_uncle are Ok;
												qed");
			});

		open_block
	}

	fn vm_factory(&self) -> &EvmFactory {
		&self.factories.vm
	}

	fn import_sealed_block(&self, block: SealedBlock) -> ImportResult {
		let h = block.header().hash();
		let start = precise_time_ns();
		let route = {
			// scope for self.import_lock
			let _import_lock = self.import_lock.lock();
			let _timer = PerfTimer::new("import_sealed_block");

			let number = block.header().number();
			let block_data = block.rlp_bytes();
			let route = self.commit_block(block, &h, &block_data);
			trace!(target: "client", "Imported sealed block #{} ({})", number, h);
			self.state_db.lock().sync_cache(&route.enacted, &route.retracted, false);
			route
		};
		let (enacted, retracted) = self.calculate_enacted_retracted(&[route]);
		self.miner.chain_new_blocks(self, &[h.clone()], &[], &enacted, &retracted);
		self.notify(|notify| {
			notify.new_blocks(
				vec![h.clone()],
				vec![],
				enacted.clone(),
				retracted.clone(),
				vec![h.clone()],
				precise_time_ns() - start,
			);
		});
		self.db.read().flush().expect("DB flush failed.");
		Ok(h)
	}
}

impl MayPanic for Client {
	fn on_panic<F>(&self, closure: F) where F: OnPanicListener {
		self.panic_handler.on_panic(closure);
	}
}


#[test]
fn should_not_cache_details_before_commit() {
	use tests::helpers::*;
	use std::thread;
	use std::time::Duration;
	use std::sync::atomic::{AtomicBool, Ordering};

	let client = generate_dummy_client(0);
	let genesis = client.chain_info().best_block_hash;
	let (new_hash, new_block) = get_good_dummy_block_hash();

	let go = {
		// Separate thread uncommited transaction
		let go = Arc::new(AtomicBool::new(false));
		let go_thread = go.clone();
		let another_client = client.reference().clone();
		thread::spawn(move || {
			let mut batch = DBTransaction::new(&*another_client.chain.read().db().clone());
			another_client.chain.read().insert_block(&mut batch, &new_block, Vec::new());
			go_thread.store(true, Ordering::SeqCst);
		});
		go
	};

	while !go.load(Ordering::SeqCst) { thread::park_timeout(Duration::from_millis(5)); }

	assert!(client.tree_route(&genesis, &new_hash).is_none());
}
