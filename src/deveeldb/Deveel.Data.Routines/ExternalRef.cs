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
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml.Linq;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Routines {
	public class ExternalRef {
		private Type[] argTypes;
		private MethodInfo methodInfo;

		public ExternalRef(string typeName, string methodName)
			: this(GetAssemblyName(typeName), typeName, methodName) {
		}

		public ExternalRef(string assemblyName, string typeName, string methodName)
			: this(assemblyName, typeName, methodName, new string[0]) {
		}

		public ExternalRef(string typeName, string methodName, string[] argumentTypes) 
			: this(GetAssemblyName(typeName), typeName, methodName, argumentTypes) {
		}

		public ExternalRef(string assemblyName, string typeName, string methodName, string[] argumentTypes) {
			if (typeName == null)
				throw new ArgumentNullException("typeName");
			if (String.IsNullOrEmpty(methodName))
				throw new ArgumentNullException("methodName");

			AssemblyName = assemblyName;
			TypeName = typeName;
			MethodName = methodName;
			ArgumentTypeNames = argumentTypes;
		}

		public string AssemblyName { get; private set; }

		public string TypeName { get; private set; }

		public string TypeString {
			get { return String.Format("{0}, {1}", TypeName, AssemblyName); }
		}

		public Type Type {
			get { return Type.GetType(TypeString, false); }
		}

		public string MethodName { get; private set; }

		public string[] ArgumentTypeNames { get; private set; }

		public Type[] ArgumentTypes {
			get {
				if (argTypes == null)
					argTypes = GetArgumentTypes(ArgumentTypeNames);

				return argTypes;
			}
		}

		private static Type[] GetArgumentTypes(string[] names) {
			if (names == null || names.Length == 1)
				return new Type[0];

			var types = new Type[names.Length];

			for (int i = 0; i < names.Length; i++) {
				var typeName = names[i];

				// TODO: support more advanced types than those resolved in the AppDomain
				types[i] = ResolveType(typeName);
			}

			return types;
		}

		private static Type ResolveType(string typeName) {
			if (typeName.IndexOf('[') != -1) {
				var delim1 = typeName.IndexOf('[');
				var delim2 = typeName.IndexOf(']');

				//TODO: support multi-dimensional arrays?
				var elemTypeName = typeName.Substring(0, delim1);
				var elemType = ResolveType(elemTypeName);

				return elemType.MakeArrayType(1);
			}

			switch (typeName) {
				case "bool":
					return typeof (bool);
				case "byte":
					return typeof (byte);
				case "short":
					return typeof (short);
				case "int":
					return typeof (int);
				case "long":
					return typeof (long);
				case "float":
					return typeof (float);
				case "double":
					return typeof (double);
				case "DateTime":
					return typeof (DateTime);
				case "DateTimeOffset":
					return typeof (DateTimeOffset);
				case "string":
					return typeof (string);
				default:
					return Type.GetType(typeName, true);
			}
		}

		private static string GetAssemblyName(string typeName) {
			string assemblyName = null;
			var comma = typeName.IndexOf(',');
			if (comma != -1) {
				assemblyName = typeName.Substring(comma + 1);
				if (!String.IsNullOrEmpty(assemblyName))
					assemblyName = assemblyName.Trim();
			}

			return assemblyName;
		}

		public MethodInfo GetMethod() {
			if (methodInfo == null) {
				var paramTypes = ArgumentTypes;
				var methodName = MethodName;
				var type = Type;

				if (type == null)
					return null;

				MethodInfo foundMethod = null;

#if PCL
				var methods = type.GetRuntimeMethods().Where(x => x.IsPublic && x.IsStatic);
#else
				var methods = type.GetMethods(BindingFlags.Public | BindingFlags.Static);
#endif

				foreach (var method in methods) {
					if (!String.Equals(method.Name, methodName))
						continue;

					var methodArgTypes = method.GetParameters()
						.Select(x => x.ParameterType).ToArray();

					if (TypesMatch(methodArgTypes, paramTypes)) {
						if (foundMethod != null)
							throw new AmbiguousMatchException();

						foundMethod = method;
					}
				}

				methodInfo = foundMethod;
			}

			return methodInfo;
		}

		internal void CheckReference(RoutineInfo routineInfo) {
			var method = GetMethod();
			if (method == null)
				throw new ArgumentException(String.Format("The reference '{0}' does not resolve to any method.", this));

			if (routineInfo is FunctionInfo &&
			    method.ReturnType == typeof (void)) {
				throw new ArgumentException(String.Format("The method '{0}.{1}' is not a function.", Type.FullName, method.Name));
			} else if (routineInfo is ProcedureInfo &&
			           method.ReturnType != typeof (void)) {
				throw new ArgumentException(String.Format("The method '{0}.{1}' is not a procedure.", Type.FullName, method.Name));
			}

			var methodParams = method.GetParameters();
			if (!CheckParametersMatch(routineInfo, methodParams))
				throw new ArgumentException("The parameters of this function and the reference do not match.");

		}

		private bool CheckParametersMatch(RoutineInfo routineInfo, ParameterInfo[] parameters) {
			var routineParameters = routineInfo.Parameters;
			if ((routineParameters == null || routineParameters.Length == 0) &&
				(parameters == null || parameters.Length == 0))
				return true;

			if (routineParameters == null || routineParameters.Length == 0)
				return false;

			var offset = 0;
			if (parameters[0].ParameterType == typeof(ISession))
				offset = 1;

			if (routineParameters.Length != parameters.Length - offset)
				return false;

			for (int i = offset; i < parameters.Length; i++) {
				var param = parameters[i];

				var paramType = PrimitiveTypes.FromType(param.ParameterType);
				var routineParameter = routineParameters[i];

				if (!routineParameter.Type.CanCastTo(paramType))
					return false;
			}

			return true;
		}


		private static bool TypesMatch(Type[] methodTypes, Type[] refTypes) {
			if ((methodTypes == null || methodTypes.Length == 0) &&
			    (refTypes == null || refTypes.Length == 0))
				return true;

			if (methodTypes == null || methodTypes.Length == 0)
				return false;

			if (methodTypes[0] == typeof (IRequest) ||
				methodTypes[0] == typeof(IQuery) ||
				methodTypes[0] == typeof(ISession)) {
				var temp = new Type[methodTypes.Length - 1];
				Array.Copy(methodTypes, 1, temp, 0, methodTypes.Length - 1);
			}

			if (methodTypes.Length != refTypes.Length)
				return false;

			for (int i = 0; i < methodTypes.Length; i++) {
				if (methodTypes[i] != refTypes[i])
					return false;
			}

			return true;
		}

		public string ToString(bool includeAssembly) {
			var sb = new StringBuilder();
			if (includeAssembly && !String.IsNullOrEmpty(AssemblyName))
				sb.AppendFormat("[{0}]", AssemblyName);

			sb.Append(TypeName);
			sb.Append('.');
			sb.Append(MethodName);
			sb.Append('(');

			if (ArgumentTypeNames != null) {
				for (int i = 0; i < ArgumentTypeNames.Length; i++) {
					sb.Append(ArgumentTypeNames[i]);

					if (i < ArgumentTypeNames.Length - 1)
						sb.Append(", ");
				}
			}

			sb.Append(')');

			return sb.ToString();
		}

		public override string ToString() {
			return ToString(true);
		}

		private static bool TryParse(string s, out ExternalRef externalRef, out Exception error) {
			externalRef = null;

			if (String.IsNullOrEmpty(s)) {
				error = new ArgumentNullException("s");
				return false;
			}

			var typeName = s;
			string assemblyName = null;

			if (s[0] == '[') {
				var delim2 = s.IndexOf(']');
				if (delim2 == -1) {
					error = new FormatException("The external ref is invalid.");
					return false;
				}

				assemblyName = s.Substring(1, delim2-1);
				typeName = s.Substring(delim2 + 1);
			}

			var delim = typeName.LastIndexOf('.');
			if (delim == -1) {
				error = new FormatException("The input type is invalid.");
				return false;
			}

			var methodName = typeName.Substring(delim + 1);
			typeName = typeName.Substring(0, delim);

			var parenDelim1 = methodName.IndexOf('(');
			if (parenDelim1 == -1) {
				error = new FormatException("Method open parenthesis not found.");
				return false;
			}

			var parenDelim2 = methodName.IndexOf(')');
			if (parenDelim2 == -1) {
				error = new FormatException("Method close parenthesis not found.");
				return false;
			}

			var argString = methodName.Substring(parenDelim1 + 1, (parenDelim2 - parenDelim1) - 1);
			methodName = methodName.Substring(0, parenDelim1);

			var args = new List<string>();

			if (!String.IsNullOrEmpty(argString)) {
				var sp = argString.Split(',');
				for (int i = 0; i < sp.Length; i++) {
					var arg = sp[i].Trim();
					if (String.IsNullOrEmpty(arg)) {
						error = new FormatException("One of the arguments is invalid.");
						return false;
					}

					args.Add(arg);
				}
			}

			externalRef = new ExternalRef(assemblyName, typeName, methodName, args.ToArray());

			error = null;
			return true;
		}

		public static bool TryParse(string s, out ExternalRef externalRef) {
			Exception error;
			return TryParse(s, out externalRef, out error);
		}

		public static ExternalRef Parse(string s) {
			ExternalRef externalRef;
			Exception error;

			if (!TryParse(s, out externalRef, out error))
				throw error;

			return externalRef;
		}

		public static ExternalRef MakeRef(Type type, string methodInfo) {
			return MakeRef(type, methodInfo, true);
		}

		public static ExternalRef MakeRef(Type type, string methodInfo, bool includeAssembly) {
			if (type == null)
				throw new ArgumentNullException("type");

			string assemblyName = null;

#if PCL
			if (includeAssembly)
				assemblyName = type.GetTypeInfo().Assembly.FullName;
#else
			if (includeAssembly)
				assemblyName = type.Assembly.FullName;
#endif

			var typeName = type.FullName;

			var parenDelim1 = methodInfo.IndexOf('(');
			if (parenDelim1 == -1)
				throw new FormatException("Method open parenthesis not found.");

			var parenDelim2 = methodInfo.IndexOf(')');
			if (parenDelim2 == -1)
				throw new FormatException("Method close parenthesis not found.");

			var argString = methodInfo.Substring(parenDelim1 + 1, (parenDelim2 - parenDelim1) - 1);
			var methodName = methodInfo.Substring(0, parenDelim1);

			var args = new List<string>();

			if (!String.IsNullOrEmpty(argString)) {
				var sp = argString.Split(',');
				for (int i = 0; i < sp.Length; i++) {
					var arg = sp[i].Trim();
					if (String.IsNullOrEmpty(arg))
						throw new FormatException("One of the arguments is invalid.");

					args.Add(arg);
				}
			}

			return new ExternalRef(assemblyName, typeName, methodName, args.ToArray());
		}
	}
}
