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
using System.Text;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql;

namespace Deveel.Data.Routines {
	/// <summary>
	/// Defines the metadata for a routine that are used to
	/// resolve within a context.
	/// </summary>
	public abstract class RoutineInfo : IObjectInfo {
		/// <summary>
		/// Constructs a routine info with the given name.
		/// </summary>
		/// <param name="routineName">The name uniquely identifying the routine.</param>
		protected RoutineInfo(ObjectName routineName) 
			: this(routineName, new RoutineParameter[] {}) {
		}

		/// <summary>
		/// Constructs the routine info with the given signature.
		/// </summary>
		/// <param name="routineName">The name uniquely identifying the routine.</param>
		/// <param name="parameters">The list of parameter information of the routine.</param>
		protected RoutineInfo(ObjectName routineName, RoutineParameter[] parameters) {
			if (routineName == null)
				throw new ArgumentNullException("routineName");

			if (parameters == null)
				parameters = new RoutineParameter[0];

			RoutineName = routineName;
			Parameters = parameters;
		}

		DbObjectType IObjectInfo.ObjectType {
			get { return ObjectType; }
		}

		ObjectName IObjectInfo.FullName {
			get { return RoutineName; }
		}

		/// <summary>
		/// Gets the name of the routine that uniquely identifies it in a system context.
		/// </summary>
		public ObjectName RoutineName { get; private set; }

		protected abstract DbObjectType ObjectType { get; }

		/// <summary>
		/// Gets an array of parameters for the routine.
		/// </summary>
		public RoutineParameter[] Parameters { get; private set; }

		public string ExternalMethodName { get; set; }

		public Type ExternalType { get; set; }

		internal abstract bool MatchesInvoke(Invoke request, IQueryContext queryContext);

		public override string ToString() {
			var sb = new StringBuilder();
			sb.Append(RoutineName);
			if (Parameters != null && Parameters.Length > 0) {
				sb.Append('(');
				for (int i = 0; i < Parameters.Length; i++) {
					sb.Append(Parameters[i]);

					if (i < Parameters.Length - 1)
						sb.Append(", ");
				}
				sb.Append(')');
			}
			return sb.ToString();
		}
	}
}