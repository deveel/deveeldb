// 
//  ObjectInstantiation.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Reflection;

using Deveel.Data;

namespace Deveel.Data.Functions {
	// Instantiates a new object.
	internal sealed class ObjectInstantiation : Function {
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
}