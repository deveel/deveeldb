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

using Deveel.Data.Util;

namespace Deveel.Data.Protocol {
	/// <summary>
	/// Represents a stream that supports required functionalities
	/// for a <see cref="LengthMarkedBufferedInputStream"/>
	/// </summary>
	public interface IInputStream {
		/// <summary>
		/// Gets ths available bytes to be read on the underlying stream.
		/// </summary>
		int Available { get; }

		int Read(byte[] bytes, int offset, int length);
	}
}