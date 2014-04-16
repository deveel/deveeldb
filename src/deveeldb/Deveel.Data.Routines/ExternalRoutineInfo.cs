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
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

using Deveel.Data.Types;

namespace Deveel.Data.Routines {
 	public sealed class ExternalRoutineInfo {
 		public ExternalRoutineInfo(string typeString, string methodName) 
			: this(typeString, methodName, new string[0]) {
 		}

 		public ExternalRoutineInfo(string typeString, string methodName, string[] argNames) {
 			this.TypeName = typeString;
 			this.MethodName = methodName;
 			this.Arguments = argNames;
 		}

 		public string TypeName { get; private set; }

		public string MethodName { get; private set; }

 		public bool HasMethodName {
			get { return !String.IsNullOrEmpty(MethodName); }
 		}

		public string[] Arguments { get; private set; }

		/// <summary>
		/// Resolves a type specification string to a <see cref="Type"/>.
		/// </summary>
		/// <param name="typeString"></param>
		/// <returns></returns>
		private static Type ResolveToType(string typeString) {
			// Trim the string
			typeString = typeString.Trim();

			// Is this an array?  Count the number of array dimensions.
			int dimensions = -1;
			int lastIndex = typeString.Length;
			while (lastIndex > 0) {
				++dimensions;
				lastIndex = typeString.LastIndexOf("[]", lastIndex, StringComparison.InvariantCulture) - 1;
			}

			// Remove the array part
			int arrayEnd = typeString.Length - (dimensions * 2);
			String typePart = typeString.Substring(0, arrayEnd);
			// Check there's no array parts in the class part
			if (typePart.IndexOf("[]", StringComparison.InvariantCulture) != -1)
				throw new Exception("Type specification incorrectly formatted: " + typeString);

			// Convert the specification to a .NET Type.  For example,
			// String is converted to typeof(System.String), etc.
			Type cl;
			// Is there a '.' in the class specification?
			if (typePart.IndexOf('.') != -1) {
				// Must be a specification such as 'System.Uri' or 'System.Collection.IList'.
				try {
					cl = Type.GetType(typePart);
				} catch (TypeLoadException) {
					throw new Exception("Type not found: " + typePart);
				}
			}

				// Try for a primitive types
			else if (typePart.Equals("boolean") ||
					 typePart.Equals("bool")) {
				cl = typeof(bool);
			} else if (typePart.Equals("byte")) {
				cl = typeof(byte);
			} else if (typePart.Equals("short")) {
				cl = typeof(short);
			} else if (typePart.Equals("char")) {
				cl = typeof(char);
			} else if (typePart.Equals("int")) {
				cl = typeof(int);
			} else if (typePart.Equals("long")) {
				cl = typeof(long);
			} else if (typePart.Equals("float")) {
				cl = typeof(float);
			} else if (typePart.Equals("double")) {
				cl = typeof(double);
			} else {
				// Not a primitive type so try resolving against System.* or some
				// key classes in Deveel.Data.*
				if (typePart.Equals("IProcedureConnection")) {
					cl = typeof(IProcedureConnection);
				} else {
					try {
						cl = Type.GetType("System." + typePart);
					} catch (TypeLoadException) {
						// No luck so give up,
						throw new Exception("Type not found: " + typePart);
					}
				}
			}

			// Finally make into a dimension if necessary
			if (dimensions > 0) {
				// This is a little untidy way of doing this.  Perhaps a better approach
				// would be to make an array encoded string.
				cl = Array.CreateInstance(cl, new int[dimensions]).GetType();
			}

			return cl;
		}


 		public MethodInfo ResolveMethod(TType[] paramTypes) {
			// The name of the class
			String typeName;
			// The name of the invokation method in the class.
			String methodName;
			// The object specification that must be matched.  If any entry is 'null'
			// then the argument parameter is discovered.
			Type[] argTypes;
			bool firstProcedureConnectionIgnore;

			if (!HasMethodName) {
				// This means the typeString only specifies a class name, so we use
				// 'Invoke' as the static method to call, and discover the arguments.
				typeName = TypeName;
				methodName = "Invoke";
				// All null which means we discover the arg types dynamically
				argTypes = new Type[paramTypes.Length];
				// ignore IProcedureConnection is first argument
				firstProcedureConnectionIgnore = true;
			} else {
				// This means we specify a class and method name and argument
				// specification.
				typeName = TypeName;
				methodName = MethodName;
				argTypes = new Type[Arguments.Length];

				for (int i = 0; i < Arguments.Length; ++i) {
					String typeSpec = Arguments[i];
					argTypes[i] = ResolveToType(typeSpec);
				}

				firstProcedureConnectionIgnore = false;
			}

			Type routineType = Type.GetType(typeName, false, true);
			if (routineType == null)
				throw new Exception("Procedure class not found: " + typeName);

			// Get all the methods in this class
			MethodInfo[] methods = routineType.GetMethods(BindingFlags.Public | BindingFlags.Static);
			MethodInfo invokeMethod = null;
			// Search for the invoke method
			foreach (MethodInfo method in methods) {
				if (method.Name.Equals(methodName)) {
					bool paramsMatch;

					// Get the parameters for this method
					ParameterInfo[] methodArgs = method.GetParameters();

					// If no methods, and object_specification has no args then this is a
					// match.
					if (methodArgs.Length == 0 && argTypes.Length == 0) {
						paramsMatch = true;
					} else {
						int searchStart = 0;
						// Is the first arugments a IProcedureConnection implementation?
						if (firstProcedureConnectionIgnore &&
							typeof(IProcedureConnection).IsAssignableFrom(methodArgs[0].ParameterType)) {
							searchStart = 1;
						}

						// Do the number of arguments match
						if (argTypes.Length == methodArgs.Length - searchStart) {
							// Do they match the specification?
							bool matchSpec = true;
							for (int n = 0; n < argTypes.Length && matchSpec; ++n) {
								Type argType = argTypes[n];
								if (argType != null &&
									argType != methodArgs[n + searchStart].ParameterType) {
									matchSpec = false;
								}
							}
							paramsMatch = matchSpec;
						} else {
							paramsMatch = false;
						}
					}

					if (paramsMatch) {
						if (invokeMethod != null)
							throw new Exception("Ambiguous public static " + methodName + " methods in stored procedure class '" + typeName + "'");

						invokeMethod = method;
					}
				}
			}

			// Return the invoke method we found
			return invokeMethod;
 		}

 		public static ExternalRoutineInfo Parse(string s) {
			// Look for the first parenthese
			int parentheseDelim = s.IndexOf("(", StringComparison.InvariantCulture);

 			if (parentheseDelim != -1) {
				// This represents type/method
				string typeMethod = s.Substring(0, parentheseDelim);
				// This will be deliminated by a '.'
				int methodDelim = typeMethod.LastIndexOf(".", StringComparison.InvariantCulture);
				if (methodDelim == -1)
					throw new FormatException("Incorrectly formatted method string: " + s);

				string typeString = typeMethod.Substring(0, methodDelim);
				string methodString = typeMethod.Substring(methodDelim + 1);

				// Next parse the argument list
				int endParentheseDelim = s.LastIndexOf(")", StringComparison.InvariantCulture);
				if (endParentheseDelim == -1)
					throw new FormatException("Incorrectly formatted method string: " + s);

				string argListStr = s.Substring(parentheseDelim + 1, endParentheseDelim - (parentheseDelim + 1));

				// Now parse the list of arguments
				string[] args = argListStr.Split(new char[]{','}, StringSplitOptions.RemoveEmptyEntries);

				return new ExternalRoutineInfo(typeString, methodString, args);
			}

			// No parenthese so we assume this is a class
			return new ExternalRoutineInfo(s, null);
 		}
 	}
}
