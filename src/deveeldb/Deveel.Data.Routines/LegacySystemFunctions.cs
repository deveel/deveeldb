// 
//  Copyright 2010-2014  Deveel
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
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;

using Deveel.Data.DbSystem;
using Deveel.Data.Query;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Text;
using Deveel.Data.Types;
using Deveel.Data.Util;
using Deveel.Math;

namespace Deveel.Data.Routines {
	public static class LegacySystemFunctions {
		private static LegacyFunctionFactory factory;

		public static LegacyFunctionFactory Factory {
			get {
				if (factory == null) {
					factory = new SystemFunctionsFactory();
					factory.Init();
				}

				return factory;
			}
		}

		#region SystemFunctionsFactory

		class SystemFunctionsFactory : LegacyFunctionFactory {
			protected override void OnInit() {
				// Internal Functions
				// Object instantiation (Internal)
				AddFunction("_new_Object", typeof(ObjectInstantiation2));

				// crypto
				AddFunction("hash", typeof(HashInvokedFunction));
			}
		}

		#endregion

		#region ObjectInstantiation

		// Instantiates a new object.
		[Serializable]
		class ObjectInstantiation : InvokedFunction {
			public ObjectInstantiation(Expression[] parameters)
				: base("_new_Object", parameters) {

				if (ParameterCount < 1) {
					throw new Exception("_new_Object function must have one argument.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				// Resolve the parameters...
				int arg_len = ParameterCount - 1;
				Object[] args = new Object[arg_len];
				for (int i = 0; i < args.Length; ++i) {
					args[i] = this[i + 1].Evaluate(group, resolver,
												   context).Object;
				}
				Object[] casted_args = new Object[arg_len];

				try {
					String typeName = this[0].Evaluate(null, resolver, context).Object.ToString();
					Type c = Type.GetType(typeName);

					ConstructorInfo[] constructs = c.GetConstructors();
					// Search for the first constructor that we can use with the given
					// arguments.
					// search_constructs:
					for (int i = 0; i < constructs.Length; ++i) {
						ParameterInfo[] construct_args = constructs[i].GetParameters();
						if (construct_args.Length == arg_len) {
							for (int n = 0; n < arg_len; ++n) {
								// If we are dealing with a primitive,
								if (construct_args[n].ParameterType.IsPrimitive) {
									String class_name = construct_args[n].ParameterType.Name;
									// If the given argument is a number,
									if (Caster.IsNumber(args[n])) {
										if (class_name.Equals("byte")) {
											casted_args[n] = Convert.ToByte(args[n]);
										} else if (class_name.Equals("char")) {
											casted_args[n] = Convert.ToChar((int)args[n]);
										} else if (class_name.Equals("double")) {
											casted_args[n] = Convert.ToDouble(args[n]);
										} else if (class_name.Equals("float")) {
											casted_args[n] = Convert.ToSingle(args[n]);
										} else if (class_name.Equals("int")) {
											casted_args[n] = Convert.ToInt32(args[n]);
										} else if (class_name.Equals("long")) {
											casted_args[n] = Convert.ToInt64(args[n]);
										} else if (class_name.Equals("short")) {
											casted_args[n] = Convert.ToInt16(args[n]);
										} else {
											// Can't cast the primitive type to a number so break,
											// break search_constructs;
											break;
										}

									}
										// If we are a bool, we can cast to primitive bool
									else if (args[n] is Boolean) {
										// If primitive type constructor arg is a bool also
										if (class_name.Equals("bool")) {
											casted_args[n] = args[n];
										} else {
											// break search_constructs;
											break;
										}
									}
										// Otherwise we can't cast,
									else {
										// break search_constructs;
										break;
									}

								}
									// Not a primitive type constructor arg,
								else {
									// PENDING: Allow string -> char conversion
									if (construct_args[n].ParameterType.IsInstanceOfType(args[n])) {
										casted_args[n] = args[n];
									} else {
										// break search_constructs;
										break;
									}
								}
							}  // for (int n = 0; n < arg_len; ++n)
							// If we get here, we have a match...
							Object ob = constructs[i].Invoke(casted_args);
							ByteLongObject serialized_ob = ObjectTranslator.Serialize(ob);
							return new TObject(new TObjectType(typeName), serialized_ob);
						}
					}

					throw new Exception(
						"Unable to find a constructor for '" + typeName +
						"' that matches given arguments.");

				} catch (TypeLoadException e) {
					throw new Exception("Type not found: " + e.Message);
				} catch (TypeInitializationException e) {
					throw new Exception("Instantiation ApplicationException: " + e.Message);
				} catch (AccessViolationException e) {
					throw new Exception("Illegal Access ApplicationException: " + e.Message);
				} catch (ArgumentException e) {
					throw new Exception("Illegal Argument ApplicationException: " + e.Message);
				} catch (TargetInvocationException e) {
					throw new Exception("Invocation Target ApplicationException: " + e.Message);
				}

			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				String clazz = this[0].Evaluate(null, resolver,
												context).Object.ToString();
				return new TObjectType(clazz);
			}

		}

		#endregion

		#region ObjectInstantiation2

		[Serializable]
		class ObjectInstantiation2 : InvokedFunction {
			public ObjectInstantiation2(Expression[] parameters)
				: base("_new_Object", parameters) {

				if (ParameterCount < 1) {
					throw new Exception("_new_Object function must have one argument.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				// Resolve the parameters...
				int arg_len = ParameterCount - 1;
				TObject[] args = new TObject[arg_len];
				for (int i = 0; i < args.Length; ++i) {
					args[i] = this[i + 1].Evaluate(group, resolver, context);
				}
				Caster.DeserializeObjects(args);

				try {
					// Get the class name of the object to be constructed
					String clazz = this[0].Evaluate(null, resolver,
													context).Object.ToString();
					Type c = Type.GetType(clazz);
					ConstructorInfo[] constructs = c.GetConstructors();

					ConstructorInfo bestConstructor =
						Caster.FindBestConstructor(constructs, args);
					if (bestConstructor == null) {
						// Didn't find a match - build a list of class names of the
						// args so the user knows what we were looking for.
						String argTypes = Caster.GetArgTypesString(args);
						throw new Exception(
							"Unable to find a constructor for '" + clazz +
							"' that matches given arguments: " + argTypes);
					}
					Object[] casted_args =
						Caster.CastArgsToConstructor(args, bestConstructor);
					// Call the constructor to create the object
					Object ob = bestConstructor.Invoke(casted_args);
					ByteLongObject serialized_ob = ObjectTranslator.Serialize(ob);
					return new TObject(new TObjectType(clazz), serialized_ob);

				} catch (TypeLoadException e) {
					throw new Exception("Class not found: " + e.Message);
				} catch (TypeInitializationException e) {
					throw new Exception("Instantiation ApplicationException: " + e.Message);
				} catch (AccessViolationException e) {
					throw new Exception("Illegal Access ApplicationException: " + e.Message);
				} catch (ArgumentException e) {
					throw new Exception("Illegal Argument ApplicationException: " + e.Message);
				} catch (TargetInvocationException e) {
					String msg = e.Message;
					if (msg == null) {
						Exception th = e.InnerException;
						if (th != null) {
							msg = th.GetType().Name + ": " + th.Message;
						}
					}
					throw new Exception("Invocation Target ApplicationException: " + msg);
				}

			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				String clazz = this[0].Evaluate(null, resolver,
												context).Object.ToString();
				return new TObjectType(clazz);
			}

		}

		#endregion

		#region HashFunction

		class HashInvokedFunction : InvokedFunction {
			public HashInvokedFunction(Expression[] parameters)
				: base("hash", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				var functionName = this[0].Evaluate(group, resolver, context);

				if (functionName.IsNull)
					throw new InvalidOperationException("Hash function name required.");

				var hash = HashFunctions.GetFunction((string)functionName.Object);
				if (hash == null)
					throw new NotSupportedException(String.Format("Hash function {0} is not supported by the system.", functionName));

				var data = this[1].Evaluate(group, resolver, context);

				if (data.TType is TBinaryType) {
					var str = data.ToStringValue();
					var result = hash.ComputeString(str);
					return TObject.CreateString(result);
				}
				if (data.TType is TStringType) {
					var blob = (ByteLongObject)data.Object;
					var result = hash.Compute(blob.ToArray());
					return new TObject(TType.GetBinaryType(SqlType.Binary, result.Length), result);
				}

				throw new InvalidOperationException("Data type argument not supported");
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return this[1].ReturnTType(resolver, context);
			}
		}

		#endregion
	}
}