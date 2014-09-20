// 
//  Copyright 2010-2014 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;

namespace Deveel.Data.Store {
	/// <summary>
	/// An interface for an area that can be modified.
	/// </summary>
	/// <remarks>
	/// Any changes made to an area may or may not be immediately reflected in 
	/// already open areas with the same id.  The specification does guarantee 
	/// that after the <see cref="CheckOut"/> method is invoked that any new 
	/// <see cref="IArea"/> or <see cref="IMutableArea"/> objects created by the 
	/// backing store will contain the changes.
	/// </remarks>
	public interface IMutableArea : IArea {
		/// <summary>
		/// Checks out all changes made to this area.
		/// </summary>
		/// <remarks>
		/// This should be called after a series of updates have been made to the 
		/// area and the final change is to be 'finalized'.  When this method returns, 
		/// any new <see cref="IArea"/> or <see cref="IMutableArea"/> objects created by 
		/// the backing store will contain the changes made to this object.  Any changes 
		/// made to the <see cref="IArea"/> may or may not be made to any already existing 
		/// areas.
		/// <para>
		/// In a logging implementation, this may flush out the changes made to the area 
		/// in a log.
		/// </para>
		/// </remarks>
		void CheckOut();

		// ---------- Various write methods ----------

		void WriteByte(byte value);

		void Write(byte[] buf, int off, int len);

		void Write(byte[] buf);

		void WriteInt2(short value);

		void WriteInt4(int value);

		void WriteInt8(long value);

		void WriteChar(char value);
	}
}