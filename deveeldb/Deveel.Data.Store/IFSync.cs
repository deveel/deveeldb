//  
//  IFSync.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.IO;

namespace Deveel.Data.Store {
	/// <summary>
	/// An interface providing a contract for the synchronization
	/// of a <see cref="FileStream"/> with the underlying file-system.
	/// </summary>
	public interface IFSync {
		/// <summary>
		/// Synchronizes the latest modifications on the given
		/// <see cref="FileStream"/> to the underlying file-system.
		/// </summary>
		/// <param name="stream">The <see cref="FileStream"/> to synchronize
		/// with the underlying file-system.</param>
		/// <exception cref="SyncFailedException">
		/// If the synchronization operation fails for any reason.
		/// </exception>
		void Sync(FileStream stream);
	}
}