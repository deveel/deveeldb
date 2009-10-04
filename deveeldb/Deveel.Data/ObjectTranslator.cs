//  
//  ObjectTranslator.cs
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
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

using Deveel.Math;

namespace Deveel.Data {
	/// <summary>
	/// This object compliments <see cref="ObjectTransfer"/> and provides a 
	/// method to translate any object into a type the database engine can process.
	/// </summary>
	public class ObjectTranslator {
		/// <summary>
		/// Translates the given object to a type the database can process.
		/// </summary>
		/// <param name="ob"></param>
		/// <returns></returns>
		public static Object Translate(Object ob) {
			if (ob == null)
				return null;
			if (ob is String)
				return StringObject.FromString((String)ob);
			if (ob is StringObject ||
				ob is BigNumber ||
				ob is DateTime ||
				ob is ByteLongObject ||
				ob is Boolean ||
				ob is StreamableObject)
				return ob;
			if (ob is byte[])
				return new ByteLongObject((byte[])ob);
			if (Attribute.IsDefined(ob.GetType(), typeof(SerializableAttribute)))
				return Serialize(ob);
			throw new ApplicationException("Unable to translate object.  " +
										   "It is not a primitive type or serializable.");
		}

		///<summary>
		/// Serializes the object to a <see cref="ByteLongObject"/>.
		///</summary>
		///<param name="ob"></param>
		///<returns></returns>
		///<exception cref="ApplicationException"></exception>
		public static ByteLongObject Serialize(Object ob) {
			try {
				MemoryStream bout = new MemoryStream();
				BinaryFormatter formatter = new BinaryFormatter();
				formatter.Serialize(bout, ob);
				return new ByteLongObject(bout.ToArray());
			} catch (IOException e) {
				throw new ApplicationException("Serialization error: " + e.Message);
			}
		}

		/// <summary>
		/// Deserializes a <see cref="ByteLongObject"/> to an object.
		/// </summary>
		/// <param name="blob"></param>
		/// <returns></returns>
		public static Object Deserialize(ByteLongObject blob) {
			if (blob == null)
				return null;
			try {
				MemoryStream bin = new MemoryStream(blob.ToArray());
				BinaryFormatter formatter = new BinaryFormatter();
				return formatter.Deserialize(bin);
			} catch (TypeLoadException e) {
				throw new ApplicationException("Type not found: " + e.Message);
			} catch (IOException e) {
				throw new ApplicationException("De-serialization error: " + e.Message);
			}
		}
	}
}