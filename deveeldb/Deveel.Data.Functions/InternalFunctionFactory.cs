//  
//  InternalFunctionFactory.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;
using System.Reflection;
using System.Text;

using Deveel.Data.Client;
using Deveel.Math;

namespace Deveel.Data.Functions {
	/// <summary>
	/// A <see cref="FunctionFactory"/> for all internal SQL functions 
	/// (including aggregate, mathematical, string functions).
	/// </summary>
	/// <remarks>
	/// This <see cref="FunctionFactory"/> is registered with the 
	/// <see cref="DatabaseSystem"/> during initialization.
	/// </remarks>
	sealed class InternalFunctionFactory : FunctionFactory {
		public override void Init() {
			// Object instantiation (Internal)
			AddFunction("_new_Object", typeof(ObjectInstantiation2));

			// Internal functions
			AddFunction("i_frule_convert", typeof(FRuleConvertFunction));
			AddFunction("i_sql_type", typeof(SQLTypeString));
			AddFunction("i_view_data", typeof(ViewDataConvert));
			AddFunction("i_privilege_string", typeof(PrivilegeString));
		}

		#region FRuleConvertFunction

		// Used in the 'GetxxxKeys' methods in DeveelDbConnection.GetSchema to convert 
		// the update delete rule of a foreign key to the short enum.
		[Serializable]
		class FRuleConvertFunction : Function {
			public FRuleConvertFunction(Expression[] parameters)
				: base("i_frule_convert", parameters) {

				if (ParameterCount != 1)
					throw new Exception("i_frule_convert function must have one argument.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				// The parameter should be a variable reference that is resolved
				TObject ob = this[0].Evaluate(group, resolver, context);
				String str = null;
				if (!ob.IsNull) {
					str = ob.Object.ToString();
				}
				int v;
				if (str == null || str.Equals("") || str.Equals("NO ACTION")) {
					v = ImportedKey.NoAction;
				} else if (str.Equals("CASCADE")) {
					v = ImportedKey.Cascade;
				} else if (str.Equals("SET NULL")) {
					v = ImportedKey.SetNull;
				} else if (str.Equals("SET DEFAULT")) {
					v = ImportedKey.SetDefault;
				} else if (str.Equals("RESTRICT")) {
					v = ImportedKey.Restrict;
				} else {
					throw new ApplicationException("Unrecognised foreign key rule: " + str);
				}
				// Return the correct enumeration
				return TObject.GetInt4(v);
			}

		}

		#endregion

		#region SQLTypeString

		// Used to form an SQL type string that describes the SQL type and any
		// size/scale information together with it.
		[Serializable]
		class SQLTypeString : Function {
			public SQLTypeString(Expression[] parameters)
				: base("i_sql_type", parameters) {

				if (ParameterCount != 3)
					throw new Exception("i_sql_type function must have three arguments.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				// The parameter should be a variable reference that is resolved
				TObject type_string = this[0].Evaluate(group, resolver, context);
				TObject type_size = this[1].Evaluate(group, resolver, context);
				TObject type_scale = this[2].Evaluate(group, resolver, context);

				StringBuilder result_str = new StringBuilder();
				result_str.Append(type_string.ToString());
				long size = -1;
				long scale = -1;
				if (!type_size.IsNull) {
					size = type_size.ToBigNumber().ToInt64();
				}
				if (!type_scale.IsNull) {
					scale = type_scale.ToBigNumber().ToInt64();
				}

				if (size != -1) {
					result_str.Append('(');
					result_str.Append(size);
					if (scale != -1) {
						result_str.Append(',');
						result_str.Append(scale);
					}
					result_str.Append(')');
				}

				return TObject.GetString(result_str.ToString());
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return TType.StringType;
			}

		}

		#endregion

		#region ViewDataConvert

		// Used to convert view data in the system view table to forms that are
		// human understandable.  Useful function for debugging or inspecting views.
		[Serializable]
		class ViewDataConvert : Function {

			public ViewDataConvert(Expression[] parameters)
				: base("i_view_data", parameters) {

				if (ParameterCount != 2)
					throw new Exception("i_view_data function must have two arguments.");
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				// Get the parameters.  The first is a string describing the operation.
				// The second is the binary data to process and output the information
				// for.
				TObject commandObj = this[0].Evaluate(group, resolver, context);
				TObject data = this[1].Evaluate(group, resolver, context);

				String command_str = commandObj.Object.ToString();
				ByteLongObject blob = (ByteLongObject)data.Object;

				if (String.Compare(command_str, "referenced tables", true) == 0) {
					ViewDef view_def = ViewDef.DeserializeFromBlob(blob);
					IQueryPlanNode node = view_def.QueryPlanNode;
					ArrayList touched_tables = node.DiscoverTableNames(new ArrayList());
					StringBuilder buf = new StringBuilder();
					int sz = touched_tables.Count;
					for (int i = 0; i < sz; ++i) {
						buf.Append(touched_tables[i]);
						if (i < sz - 1) {
							buf.Append(", ");
						}
					}
					return TObject.GetString(buf.ToString());
				} else if (String.Compare(command_str, "plan dump", true) == 0) {
					ViewDef view_def = ViewDef.DeserializeFromBlob(blob);
					IQueryPlanNode node = view_def.QueryPlanNode;
					StringBuilder buf = new StringBuilder();
					node.DebugString(0, buf);
					return TObject.GetString(buf.ToString());
				} else if (String.Compare(command_str, "query string", true) == 0) {
					SqlQuery query = SqlQuery.DeserializeFromBlob(blob);
					return TObject.GetString(query.ToString());
				}

				return TObject.Null;

			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return TType.StringType;
			}

		}

		#endregion

		#region ObjectInstantiation

		// Instantiates a new object.
		[Serializable]
		class ObjectInstantiation : Function {
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
		class ObjectInstantiation2 : Function {
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
					// Call the constructor to create the java object
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

		#region PrivilegeString

		// Given a priv_bit number (from SYSTEM.sUSRGrant), this will return a
		// text representation of the privilege.
		[Serializable]
		class PrivilegeString : Function {

			public PrivilegeString(Expression[] parameters)
				: base("i_privilege_string", parameters) {

				if (ParameterCount != 1) {
					throw new Exception(
						"i_privilege_string function must have one argument.");
				}
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
											 IQueryContext context) {
				TObject priv_bit_ob = this[0].Evaluate(group, resolver, context);
				int priv_bit = ((BigNumber)priv_bit_ob.Object).ToInt32();
				Privileges privs = new Privileges();
				privs = privs.Add(priv_bit);
				return TObject.GetString(privs.ToString());
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return TType.StringType;
			}
		}

		#endregion
	}
}