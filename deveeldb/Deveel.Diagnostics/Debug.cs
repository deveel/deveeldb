//  
//  Debug.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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

namespace Deveel.Diagnostics {
	public sealed class Debug : IDisposable {
		private Debug(IDebugLogger logger) {
			this.logger = logger;
		}

		/// <summary>
		/// A logger to output any debugging messages.
		/// </summary>
		/// <remarks>
		/// <b>Note</b>: This <b>MUST</b> be read-only, because other objects may 
		/// retain a reference to the object.  If it is not read-only, then different 
		/// objects will be logging to different places if this reference is changed.
		/// </remarks>
		private readonly IDebugLogger logger;
		private static Debug current;

		internal static void Init(IDebugLogger logger) {
			current = new Debug(logger);
		}

		public static bool IsInterestedIn(DebugLevel level) {
			return current.logger.IsInterestedIn(level);
		}

		public static void Write(DebugLevel level, object ob, string message) {
			current.logger.Write(level, ob, message);
		}

		public static void Write(DebugLevel level, Type type, string message) {
			current.logger.Write(level, type, message);
		}

		public static void Write(DebugLevel level, string typeString, string message) {
			current.logger.Write(level, typeString, message);
		}

		public static void WriteException(Exception e) {
			current.logger.WriteException(e);
		}

		public static void WriteException(DebugLevel level, Exception e) {
			current.logger.WriteException(level, e);
		}

		internal static void SetOutput(TextWriter writer) {
			if (current == null)
				current = new Debug(new DefaultDebugLogger());
			if (current.logger is DefaultDebugLogger)
				(current.logger as DefaultDebugLogger).SetOutput(writer);
		}

		internal static void SetDebugLevel(int level) {
			if (current.logger is DefaultDebugLogger)
				(current.logger as DefaultDebugLogger).SetDebugLevel(level);
		}

		void IDisposable.Dispose() {
			if (logger != null)
				logger.Dispose();
		}

		internal static void Dispose() {
			if (current != null)
				(current as IDisposable).Dispose();
		}
	}
}