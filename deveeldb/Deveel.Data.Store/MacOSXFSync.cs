//  
//  MacOSXFSync.cs
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
	/// Synchronizes a <see cref="FileStream"/> with an underlying
	/// MacOS X platform.
	/// </summary>
	public sealed class MacOSXFSync : IFSync {
		/// <inheritdoc/>
		/// <exception cref="NotSupportedException">
		/// If the current platform is not supported. The support of
		/// fsync operations under MacOS X platform is done by a call 
		/// to Mono: if the project was not compiled with this it will
		/// throw this exception by default.
		/// </exception>
		public void Sync(FileStream stream) {
#if !MONO
			throw new NotSupportedException();
#else
			//TODO:
#endif
		}
	}
}