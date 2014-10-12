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

namespace Deveel.Data {
	/// <summary>
	/// The possible directions of a procedure parameter.
	/// </summary>
	/// <remarks>
	/// By default a <see cref="ProcedureParameter"/> is
	/// set as <see cref="Input"/>.
	/// </remarks>
	[Flags]
	public enum ParameterDirection {
		/// <summary>
		/// The parameter provides an input value to the 
		/// procedure, but won't be able to output a
		/// value eventually set.
		/// </summary>
		Input = 0x01,

		/// <summary>
		/// The parameter will not provide any input value,
		/// and will output a value set during the execution
		/// of the procedure.
		/// </summary>
		Output = 0x02,

		/// <summary>
		/// The parameter will provide an input value and will
		/// return an output value set during the execution
		/// of the procedure.
		/// </summary>
		InputOutput = Input | Output
	}
}