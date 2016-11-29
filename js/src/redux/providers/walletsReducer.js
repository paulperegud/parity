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

import { handleActions } from 'redux-actions';

const initialState = {
  wallets: {},
  walletsAddresses: [],
  filterSubId: null,
  contract: null
};

export default handleActions({
  updateWallets: (state, action) => {
    const { wallets, walletsAddresses, filterSubId } = action;

    return {
      ...state,
      wallets, walletsAddresses, filterSubId
    };
  },

  updateWalletsDetails: (state, action) => {
    const { wallets } = action;
    const prevWallets = state.wallets;

    const nextWallets = { ...prevWallets };

    Object.values(wallets).forEach((wallet) => {
      const prevWallet = prevWallets[wallet.address] || {};
      const nextWallet = nextWallets[wallet.address];

      nextWallets[wallet.address] = {
        ...prevWallet,
        ...nextWallet
      };
    });

    return {
      ...state,
      wallets: nextWallets
    };
  },

  setWalletContract: (state, action) => {
    const { contract } = action;

    return {
      ...state,
      contract
    };
  }
}, initialState);
