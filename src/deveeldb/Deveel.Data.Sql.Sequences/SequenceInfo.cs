// 
//  Copyright 2010-2016 Deveel
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

using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Sql.Sequences {
	/// <summary>
	/// Provides the meta information about a <see cref="ISequence"/> configuring
	/// its operative behavior.
	/// </summary>
	/// <seealso cref="ISequence.SequenceInfo"/>
	/// <seealso cref="ISequence"/>
	public sealed class SequenceInfo : IObjectInfo {
		private SequenceInfo(ObjectName sequenceName, SequenceType sequenceType) {
			if (sequenceName == null)
				throw new ArgumentNullException("sequenceName");

			SequenceName = sequenceName;
			Type = sequenceType;
		}

		/// <summary>
		/// Constructs a new object with the information given
		/// </summary>
		/// <param name="sequenceName"></param>
		/// <param name="startValue">The start value of the sequence</param>
		/// <param name="increment">The incremental value of the sequence, that is the
		/// value added to the current value of the sequence, each time it advances.</param>
		/// <param name="minValue">The minimum value of the sequence.</param>
		/// <param name="maxValue">The maximum value of the sequence.</param>
		/// <param name="cache">The number of items to cache.</param>
		public SequenceInfo(ObjectName sequenceName, SqlNumber startValue, SqlNumber increment, SqlNumber minValue, SqlNumber maxValue, long cache) 
			: this(sequenceName, startValue, increment, minValue, maxValue, cache, true) {
		}

		/// <summary>
		/// Constructs a new object with the information given
		/// </summary>
		/// <param name="sequenceName"></param>
		/// <param name="startValue">The start value of the sequence</param>
		/// <param name="increment">The incremental value of the sequence, that is the
		/// value added to the current value of the sequence, each time it advances.</param>
		/// <param name="minValue">The minimum value of the sequence.</param>
		/// <param name="maxValue">The maximum value of the sequence.</param>
		/// <param name="cycle">Indicates if the sequence must be cycled when it reaches
		/// the minimum or maximum value.</param>
		public SequenceInfo(ObjectName sequenceName, SqlNumber startValue, SqlNumber increment, SqlNumber minValue, SqlNumber maxValue, bool cycle) 
			: this(sequenceName, startValue, increment, minValue, maxValue, 256, cycle) {
		}

		/// <summary>
		/// Constructs a new object with the information given
		/// </summary>
		/// <param name="sequenceName"></param>
		/// <param name="startValue">The start value of the sequence</param>
		/// <param name="increment">The incremental value of the sequence, that is the
		/// value added to the current value of the sequence, each time it advances.</param>
		/// <param name="minValue">The minimum value of the sequence.</param>
		/// <param name="maxValue">The maximum value of the sequence.</param>
		/// <param name="cache">The number of items to cache.</param>
		/// <param name="cycle">Indicates if the sequence must be cycled when it reaches
		/// the minimum or maximum value.</param>
		public SequenceInfo(ObjectName sequenceName, SqlNumber startValue, SqlNumber increment, SqlNumber minValue, SqlNumber maxValue, long cache, bool cycle)
			: this(sequenceName, SequenceType.Normal) {
			StartValue = startValue;
			Increment = increment;
			MinValue = minValue;
			MaxValue = maxValue;
			Cache = cache;
			Cycle = cycle;
		}

		DbObjectType IObjectInfo.ObjectType {
			get { return DbObjectType.Sequence; }
		}

		public ObjectName SequenceName { get; private set; }

		ObjectName IObjectInfo.FullName {
			get { return SequenceName; }
		}

		public SequenceType Type { get; private set; }

		/// <summary>
		/// Gets the configured starting numeric value of a sequence. 
		/// </summary>
		public SqlNumber StartValue { get; private set; }

		/// <summary>
		/// Gets the configured incremental value, that is the value added
		/// to the current value of a sequence each time it advances.
		/// </summary>
		/// <seealso cref="ISequence.GetCurrentValue"/>
		/// <seealso cref="ISequence.NextValue"/>
		public SqlNumber Increment { get; private set; }

		/// <summary>
		/// Gets the configured minimum value of the sequence.
		/// </summary>
		public SqlNumber MinValue { get; private set; }

		/// <summary>
		/// Gets the configured maximum value of the sequence.
		/// </summary>
		/// <seealso cref="Cycle"/>
		public SqlNumber MaxValue { get; private set; }

		/// <summary>
		/// Gets the number of items of the sequence to cache.
		/// </summary>
		public long Cache { get; private set; }

		/// <summary>
		/// Gets <c>true</c> if the sequence will cycle when it reaches either
		/// <see cref="MinValue"/> or <see cref="MaxValue"/>.
		/// </summary>
		/// <seealso cref="MinValue"/>
		/// <seealso cref="MaxValue"/>
		public bool Cycle { get; private set; }

		public string Owner { get; set; }

		/// <summary>
		/// Creates an object that describes a native sequence for the table
		/// having the specified name.
		/// </summary>
		/// <param name="tableName"></param>
		/// <returns></returns>
		public static SequenceInfo Native(ObjectName tableName) {
			return new SequenceInfo(tableName, SequenceType.Native);
		}
	}
}
