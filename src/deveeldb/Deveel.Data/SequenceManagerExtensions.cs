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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data {
	public static class SequenceManagerExtensions {
		public static SqlNumber NextValue(this ISequenceManager sequenceManager, ObjectName sequenceName) {
			var sequence = sequenceManager.GetObject(sequenceName) as ISequence;
			if (sequence == null)
				throw new ObjectNotFoundException(sequenceName);

			return sequence.NextValue();
		}

		public static SqlNumber LastValue(this ISequenceManager sequenceManager, ObjectName sequenceName) {
			var sequence = sequenceManager.GetObject(sequenceName) as ISequence;
			if (sequence == null)
				throw new ObjectNotFoundException(sequenceName);

			return sequence.GetCurrentValue();
		}

		public static SqlNumber SetValue(this ISequenceManager sequenceManager, ObjectName sequenceName, SqlNumber value) {
			var sequence = sequenceManager.GetObject(sequenceName) as ISequence;
			if (sequence == null)
				throw new ObjectNotFoundException(sequenceName);

			return sequence.SetValue(value);
		}
	}
}