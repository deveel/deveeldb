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
using System.Reflection;
using System.Text;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Routines {
 	public abstract class ExternalRoutineInfo : RoutineInfo { 
 		public ExternalRoutineInfo(Type type, string methodName, Type[] argTypes)
 			: this(type.FullName, methodName, ToTypeNames(argTypes)) {
 		}

 		public ExternalRoutineInfo(string typeString, string methodName) 
			: this(typeString, methodName, new string[0]) {
 		}

 		public ExternalRoutineInfo(string typeString, string methodName, string[] argNames) {
 			TypeName = typeString;
 			MethodName = methodName;
 			Arguments = argNames;
 		}

 		public string TypeName { get; private set; }

		public string MethodName { get; private set; }

 		public bool HasMethodName {
			get { return !String.IsNullOrEmpty(MethodName); }
 		}

		public string[] Arguments { get; private set; }

 		public const string InvokeMethodName = "Invoke";

		private static string[] ToTypeNames(Type[] argTypes) {
			if (argTypes == null || argTypes.Length == 0)
				return new string[0];

			var argNames = new string[argTypes.Length];
			for (var i = 0; i < argTypes.Length; i++) {
				argNames[i] = argTypes[i].FullName;
			}

			return argNames;
		}

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
			string typePart = typeString.Substring(0, arrayEnd);
			// Check there's no array parts in the class part
			if (typePart.IndexOf("[]", StringComparison.InvariantCulture) != -1)
				throw new Exception("Type specification incorrectly formatted: " + typeString);

			// Convert the specification to a .NET Type.  For example,
			// String is converted to typeof(System.String), etc.
			Type type;

			// Is there a '.' in the type specification?
			if (typePart.IndexOf('.') != -1) {
				// Must be a specification such as 'System.Uri' or 'System.Collection.IList'.
				try {
					type = Type.GetType(typePart, false);
				} catch (TypeLoadException) {
					throw new Exception("Type not found: " + typePart);
				}
			}

				// Try for a primitive types
			else if (typePart.Equals("boolean") ||
					 typePart.Equals("bool")) {
				type = typeof(bool);
			} else if (typePart.Equals("byte")) {
				type = typeof(byte);
			} else if (typePart.Equals("short")) {
				type = typeof(short);
			} else if (typePart.Equals("char")) {
				type = typeof(char);
			} else if (typePart.Equals("int")) {
				type = typeof(int);
			} else if (typePart.Equals("long")) {
				type = typeof(long);
			} else if (typePart.Equals("float")) {
				type = typeof(float);
			} else if (typePart.Equals("double")) {
				type = typeof (double);
			} else if (typePart.Equals("DateTime")) {
				type = typeof (DateTime);
			} else {
				// Not a primitive type so try resolving against System.* or some
				// key classes in Deveel.Data.*
				if (typePart.Equals("ISession")) {
					type = typeof(ISession);
				} else {
					try {
						type = Type.GetType("System." + typePart, false);
					} catch (TypeLoadException) {
						// No luck so give up,
						throw new Exception("Type not found: " + typePart);
					}
				}
			}

			if (type == null)
				return null;

			// Finally make into a dimension if necessary
			if (dimensions > 0) {
				// This is a little untidy way of doing this.  Perhaps a better approach
				// would be to make an array encoded string.
				type = type.MakeArrayType(dimensions);
			}

			return type;
		}


 		public MethodInfo ResolveMethod(SqlType[] paramTypes) {
			string typeName;
			string methodName;

			// The object specification that must be matched.  If any entry is 'null'
			// then the argument parameter is discovered.
			Type[] argTypes;
			bool ignoreFirstArg;

			if (!HasMethodName) {
				// This means the typeString only specifies a type name, so we use
				// 'Invoke' as the static method to call, and discover the arguments.
				typeName = TypeName;
				methodName = InvokeMethodName;

				// All null which means we discover the arg types dynamically
				argTypes = new Type[paramTypes.Length];

				// ignore ISession is first argument
				ignoreFirstArg = true;
			} else {
				// This means we specify a class and method name and argument
				// specification.
				typeName = TypeName;
				methodName = MethodName;
				argTypes = new Type[Arguments.Length];

				for (int i = 0; i < Arguments.Length; ++i) {
					string typeSpec = Arguments[i];
					argTypes[i] = ResolveToType(typeSpec);
				}

				ignoreFirstArg = false;
			}

			var routineType = Type.GetType(typeName, false, true);
			if (routineType == null)
				throw new Exception(String.Format("Routine containing type '{0}' not found", typeName));

			// Get all the methods in this class
			var methods = routineType.GetMethods(BindingFlags.Public | BindingFlags.Static);
			MethodInfo invokeMethod = null;

			foreach (MethodInfo method in methods) {
				if (method.Name.Equals(methodName)) {
					bool paramsMatch;

					var methodArgs = method.GetParameters();

					// If both the reference and the method have no args then this is a match.
					if (methodArgs.Length == 0 && argTypes.Length == 0) {
						paramsMatch = true;
					} else {
						int searchStart = 0;

						// Is the first argument is a ISession implementation and we are skipping it
						if (ignoreFirstArg &&
							typeof(ISession).IsAssignableFrom(methodArgs[0].ParameterType)) {
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
							throw new AmbiguousMatchException(String.Format("Ambiguous public static '{0}' methods in type '{1}'", methodName, typeName));

						invokeMethod = method;
					}
				}
			}

			return invokeMethod;
 		}

 		internal override bool MatchesInvoke(Invoke invoke, IRequest request) {
 			throw new NotImplementedException();
 		}

 		public static ExternalRoutineInfo Resolve(string s) {
			// Look for the first parentheses
			int parentheseDelim = s.IndexOf("(", StringComparison.InvariantCulture);

 			if (parentheseDelim != -1) {
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
				string[] args = argListStr.Split(new[]{','}, StringSplitOptions.RemoveEmptyEntries);

				return new ExternalRoutineInfo(typeString, methodString, args);
			}

			// No parentheses so we assume this is a class
			return new ExternalRoutineInfo(s, null);
 		}

 		public static string FormatString(Type type) {
 			return FormatString(type, Type.EmptyTypes);
 		}

 		public static string FormatString(Type type, Type[] argTypes) {
 			return FormatString(type, null, argTypes);
 		}

 		public static string FormatString(Type type, string methodName) {
 			return FormatString(type, methodName, Type.EmptyTypes);
 		}

 		public static string FormatString(Type type, string methodName, Type[] argTypes) {
 			var routineInfo = new ExternalRoutineInfo(type, methodName, argTypes);
 			return routineInfo.ToString();
 		}

 		public override string ToString() {
 			var sb = new StringBuilder(TypeName);
 			if (HasMethodName) {
 				sb.Append('.');
 				sb.Append(MethodName);
 			}

 			sb.Append('(');
 			if (Arguments != null && Arguments.Length > 0) {
 				for (int i = 0; i < Arguments.Length; i++) {
 					sb.Append(Arguments[i]);

 					if (i < Arguments.Length - 1)
 						sb.Append(", ");
 				}
 			}

 			sb.Append(')');
 			return sb.ToString();
 		}
 	}
}
