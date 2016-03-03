// 
//  Copyright 2010-2015 Deveel
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
//


using System;

namespace Deveel.Data.Protocol {
	public static class EncryptionAlgorithms {
		public const string HmacSha256 = "HMAC-SHA256";
		public const string HmacSha512 = "HMAC-SHA512";
		public const string HmacMd5 = "HMAC-MD5";
		public const string TripleDes = "Triple DES";
		public const string Des = "DES";
	}
}