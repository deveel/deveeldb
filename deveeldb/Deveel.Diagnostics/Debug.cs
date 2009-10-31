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
#if DEBUG
			this.logger = logger;
#endif
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
#if DEBUG
			current = new Debug(logger);
#endif
		}

		public static bool IsInterestedIn(DebugLevel level) {
#if DEBUG
			return current.logger.IsInterestedIn(level);
#else
			return false;
#endif
		}

		public static void Write(DebugLevel level, object ob, string message) {
#if DEBUG
			current.logger.Write(level, ob, message);
#endif
		}

		public static void Write(DebugLevel level, Type type, string message) {
#if DEBUG
			current.logger.Write(level, type, message);
#endif
		}

		public static void Write(DebugLevel level, string typeString, string message) {
#if DEBUG
			current.logger.Write(level, typeString, message);
#endif
		}

		public static void WriteException(Exception e) {
#if DEBUG
			current.logger.WriteException(e);
#endif
		}

		public static void WriteException(DebugLevel level, Exception e) {
#if DEBUG
			current.logger.WriteException(level, e);
#endif
		}

		internal static void SetOutput(TextWriter writer) {
#if DEBUG
			if (current == null)
				current = new Debug(new DefaultDebugLogger());
			if (current.logger is DefaultDebugLogger)
				(current.logger as DefaultDebugLogger).SetOutput(writer);
#endif
		}

		internal static void SetDebugLevel(int level) {
#if DEBUG
			if (current.logger is DefaultDebugLogger)
				(current.logger as DefaultDebugLogger).SetDebugLevel(level);
#endif
		}

		void IDisposable.Dispose() {
#if DEBUG
			if (logger != null)
				logger.Dispose();
#endif
		}

		internal static void Dispose() {
#if DEBUG
			if (current != null)
				(current as IDisposable).Dispose();
#endif
		}
	}
}