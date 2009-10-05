//  
//  WindowsFSync.cs
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
	/// Microsoft Windows platform.
	/// </summary>
	public sealed class WindowsFSynch : IFSync {
#if NET_2_0
		//TODO: and under Linux?
		[System.Runtime.InteropServices.DllImport("kernel32")]
		private static extern int FlushFileBuffers(Microsoft.Win32.SafeHandles.SafeFileHandle hFile);		
#else
		[System.Runtime.InteropServices.DllImport("kernel32")]
		private static extern int FlushFileBuffers(IntPtr hFile);
#endif

		public void Sync(FileStream stream) {
#if NET_2_0
			if (FlushFileBuffers(stream.SafeFileHandle) == 0)
				throw new SyncFailedException();
#else
			if (FlushFileBuffers(stream.Handle) == 0)
				throw new SyncFailedException();
#endif

		}
	}
}