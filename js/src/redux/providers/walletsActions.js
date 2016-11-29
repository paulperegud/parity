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

import { isEqual, uniq, range } from 'lodash';

import Contract from '../../api/contract';
import { wallet as WALLET_ABI } from '../../contracts/abi';

export function attachWallets (_wallets) {
  return (dispatch, getState) => {
    const { wallets, api } = getState();

    const prevAddresses = wallets.walletsAddresses;
    const nextAddresses = Object.keys(_wallets).map((a) => a.toLowerCase()).sort();

    if (isEqual(prevAddresses, nextAddresses)) {
      return;
    }

    if (wallets.filterSubId) {
      api.eth.uninstallFilter(wallets.filterSubId);
    }

    if (nextAddresses.length === 0) {
      return dispatch(updateWallets({ wallets: {}, walletsAddresses: [], filterSubId: null }));
    }

    const filterOptions = {
      fromBlock: 0,
      toBlock: 'latest',
      address: nextAddresses
    };

    api.eth
      .newFilter(filterOptions)
      .then((filterId) => {
        dispatch(updateWallets({ wallets: _wallets, walletsAddresses: nextAddresses, filterSubId: filterId }));
      })
      .catch((error) => {
        if (process.env.NODE_ENV === 'production') {
          console.error('walletsActions::attachWallets', error);
        } else {
          throw error;
        }
      });

    fetchWalletsInfo(Object.keys(_wallets))(dispatch, getState);
  };
}

export function load (api) {
  return (dispatch, getState) => {
    const contract = new Contract(api, WALLET_ABI);

    dispatch(setWalletContract(contract));
    api.subscribe('eth_blockNumber', (error) => {
      if (error) {
        if (process.env.NODE_ENV === 'production') {
          return console.error('[eth_blockNumber] walletsActions::load', error);
        } else {
          throw error;
        }
      }

      const { filterSubId } = getState().wallets;

      if (!filterSubId) {
        return;
      }

      api.eth
        .getFilterChanges(filterSubId)
        .then((logs) => {
          parseLogs(logs)(dispatch, getState);
        })
        .catch((error) => {
          if (process.env.NODE_ENV === 'production') {
            return console.error('[getFilterChanges] walletsActions::load', error);
          } else {
            throw error;
          }
        });
    });
  };
}

function fetchWalletsInfo (updates) {
  return (dispatch, getState) => {
    if (Array.isArray(updates)) {
      const _updates = updates.reduce((updates, address) => {
        updates[address] = {
          owners: true,
          require: true,
          transactions: true,
          address
        };

        return updates;
      }, {});

      return fetchWalletsInfo(_updates)(dispatch, getState);
    }

    const { contract } = getState().wallets;
    const _updates = Object.values(updates);

    Promise
      .all(_updates.map((update) => fetchWalletInfo(contract, update)))
      .then((updates) => {
        dispatch(updateWalletsDetails(updates));
      })
      .catch((error) => {
        if (process.env.NODE_ENV === 'production') {
          return console.error('walletsActions::fetchWalletsInfo', error);
        } else {
          throw error;
        }
      });
  };
}

function fetchWalletInfo (contract, update) {
  const promises = [];

  if (update.owners) {
    promises.push(fetchWalletOwners(contract, update.address));
  }

  return Promise
    .all(promises)
    .then((updates) => {
      const wallet = { address: update.address };

      updates.forEach((update) => {
        wallet[update.key] = update.value;
      });

      return wallet;
    });
}

function fetchWalletOwners (contract, address) {
  const walletInstance = contract.at(address).instance;

  return walletInstance
    .m_numOwners.call()
    .then((mNumOwners) => {
      return Promise.all(range(mNumOwners.toNumber()).map((idx) => walletInstance.getOwner.call({}, [ idx ])));
    })
    .then((owners) => {
      return {
        key: 'owners',
        value: owners
      };
    });
}

function parseLogs (logs) {
  return (dispatch, getState) => {
    const { wallets } = getState();
    const { contract } = wallets;

    const updates = {};

    logs.forEach((log) => {
      const { address, topics } = log;
      const eventSignature = topics[0];
      const prev = updates[address] || { address };

      switch (eventSignature) {
        case [ contract.OwnerChanged.signature ]:
        case [ contract.OwnerAdded.signature ]:
        case [ contract.OwnerRemoved.signature ]:
          updates[address] = {
            ...prev,
            owners: true
          };
          return;

        case [ contract.RequirementChanged.signature ]:
          updates[address] = {
            ...prev,
            require: true
          };
          return;

        case [ contract.Confirmation.signature ]:
        case [ contract.Revoke.signature ]:
          const operation = log.params.operation.value;

          updates[address] = {
            ...prev,
            operations: uniq(
              (prev.operations || []).concat(operation)
            )
          };
          return;

        case [ contract.Deposit.signature ]:
        case [ contract.SingleTransact.signature ]:
        case [ contract.MultiTransact.signature ]:
          updates[address] = {
            ...prev,
            transactions: true
          };
          return;

        case [ contract.ConfirmationNeeded.signature ]:
          const op = log.params.operation.value;

          updates[address] = {
            ...prev,
            operations: uniq(
              (prev.operations || []).concat(op)
            )
          };
          return;
      }
    });

    fetchWalletsInfo(updates)(dispatch, getState);
  };
}

function updateWalletsDetails (wallets) {
  return {
    type: 'updateWalletsDetails',
    wallets
  };
}

function setWalletContract (contract) {
  return {
    type: 'setWalletContract',
    contract
  };
}

function updateWallets (data) {
  return {
    type: 'updateWallets',
    ...data
  };
}
