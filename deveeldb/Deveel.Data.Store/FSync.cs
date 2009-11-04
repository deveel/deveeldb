//  
//  FSync.cs
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
using System.Reflection;

namespace Deveel.Data.Store {
	/// <summary>
	/// A static class used to control the implementation of
	/// <see cref="IFSync"/> currently used by the system to
	/// synchronize a file-stream with the underlying file-system.
	/// </summary>
	public
#if NET_1_1
	sealed
#else
	static
#endif
	class FSync {

#if NET_1_1
		private FSync() {
		}
#endif

		static FSync() {
			if (current == null)
				current = GetCurrent();
		}

		private static IFSync current;
		
		/// <summary>
		/// Gets an implementation of <see cref="IFSync"/> specific
		/// for Microsoft Windows operating systems.
		/// </summary>
		/// <value>
		/// An implementation of <see cref="IFSync"/> specific
		/// for Microsoft Windows operating systems.
		/// </value>
		public static IFSync Windows {
			get { return new WindowsFSynch(); }
		}
		
		/// <summary>
		/// Gets an implementation of <see cref="IFsync"/> which
		/// can be used in UNIX-like operating systems (*BSD,
		/// Linux, etc.).
		/// </summary>
		public static IFSync Unix {
			get { return new UnixFSync(); }
		}
		
		public static IFSync MacOSX {
			get { return new MacOSXFSync(); }
		}

		internal static void SetFSync(IFSync sync) {
			current = sync;
		}

		/// <summary>
		/// Retrives the default implementation of <see cref="IFSync"/>
		/// for the current platform.
		/// </summary>
		/// <returns></returns>
		private static IFSync GetCurrent() {
			// get the plaform kind
			PlatformID platform = Environment.OSVersion.Platform;
			if (platform == PlatformID.Unix)
				return new UnixFSync();
#if !MONO
			//MacOSX
			if (((int)platform == 6))
				return new MacOSXFSync();
#endif

			// if not of special platforms, this means it must be a
			// windows platform...
			return new WindowsFSynch();
		}

		/// <summary>
		/// Calls the method <see cref="IFSync.Sync"/> of the implementation 
		/// of <see cref="IFSync"/> currently used within the system.
		/// </summary>
		/// <param name="stream">The <see cref="FileStream"/> to synchronize with the
		/// underlying file-system.</param>
		public static void Sync(FileStream stream) {
			if (current == null)
				throw new InvalidOperationException();

			current.Sync(stream);
		}

		/// <summary>
		/// Creates an instance of <see cref="IFSync"/> for the given <see cref="Type"/>.
		/// </summary>
		/// <param name="type">The type which implements the <c>fsync()</c> logic.</param>
		/// <remarks>
		/// This methods works in two ways:
		/// <list type="bullet">
		/// <item>If the given <paramref name="type"/> implements the <see cref="IFSync"/>
		/// interface, this methods instantiate the type.</item>
		/// <item>If the given <paramref name="type"/> defines a method named <c>Sync</c>
		/// and having a single argument which is assignable from <see cref="FileStream"/>,
		/// this creates a wrapper class which will invoke the method.</item>
		/// </list>
		/// <para>
		/// If the <paramref name="type"/> given has to be wrapped into a <see cref="IFSync"/>,
		/// and the method <c>Sync(FileStream)</c> is not static, this method will instatiate it 
		/// before passing it to the wrapper.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Retusn an implementation of <see cref="IFSync"/> for the given <paramref name="type"/>.
		/// </returns>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="type"/> parameter is <b>null</b>.
		/// </exception>
		/// <exception cref="ArgumentException">
		/// If an error occurred while initializing the <paramref name="type"/> or if the given
		/// <paramref name="type"/> does not implement the <c>Synch(FileStream)</c> method.
		/// </exception>
		public static IFSync Create(Type type) {
			if (type == null)
				throw new ArgumentNullException("type");

			if (typeof(IFSync).IsAssignableFrom(type))
				return (IFSync) Activator.CreateInstance(type, true);

			MemberInfo[] syncMethods = type.FindMembers(MemberTypes.Method, BindingFlags.Public | BindingFlags.NonPublic,
			                                            new MemberFilter(FilterSyncMethod), null);
			if (syncMethods.Length != 1)
				throw new ArgumentException("The type given does not implement the Sync(FileStream) method.");

			MethodInfo methodInfo = (MethodInfo) syncMethods[0];

			object obj = null;

			if (!methodInfo.IsStatic) {
				try {
					obj = Activator.CreateInstance(type, true);
				} catch (Exception e) {
					throw new ArgumentException("Unable to initialize the type '" + type + "'.", e);
				}
			}

			return new FSyncWrapper(obj, methodInfo);
		}

		private static bool FilterSyncMethod(MemberInfo memberInfo, object criteria) {
			if (!(memberInfo is MethodInfo))
				return false;
			if (memberInfo.Name != "Sync")
				return false;

			MethodInfo methodInfo = (MethodInfo) memberInfo;
			ParameterInfo[] parameterInfos = methodInfo.GetParameters();
			if (parameterInfos.Length != 1)
				return false;

			if (!typeof(FileStream).IsAssignableFrom(parameterInfos[0].ParameterType))
				return false;

			return true;
		}

#region FSyncWrapper
		private class FSyncWrapper : IFSync {
			public FSyncWrapper(object obj, MethodInfo method) {
				this.obj = obj;
				this.method = method;
			}

			private readonly object obj;
			private readonly MethodInfo method;

			public void Sync(FileStream stream) {
				try {
					method.Invoke(obj, new object[] {stream});
				} catch(Exception) {
					throw new SyncFailedException();
				}
			}
		}
#endregion
		
#region WindowsFSync
		/// <summary>
		/// Synchronizes a <see cref="FileStream"/> with an underlying
		/// Microsoft Windows platform.
		/// </summary>
		private sealed class WindowsFSynch : IFSync {
#if NET_2_0
			[System.Runtime.InteropServices.DllImport("kernel32")]
			private static extern int FlushFileBuffers(Microsoft.Win32.SafeHandles.SafeFileHandle hFile);		
#else
			[System.Runtime.InteropServices.DllImport("kernel32")]
			private static extern int FlushFileBuffers(IntPtr hFile);
#endif

			public void Sync(FileStream stream) {
#if NET_2_0
				if (FlushFileBuffers(stream.SafeFileHandle) == 0)
#else
				if (FlushFileBuffers(stream.Handle) == 0)
#endif
					throw new SyncFailedException();

			}
		}
#endregion
		
#region UnixFSync
		/// <summary>
		/// Synchronizes a <see cref="FileStream"/> with an underlying
		/// Unix platform.
		/// </summary>
		private sealed class UnixFSync : IFSync {
			public UnixFSync() {
				Type monoIO = Type.GetType("System.IO.MonoIO");
				if (monoIO != null)
					monoIOSyncMethod = monoIO.GetMethod("Flush");
			}

			private MethodInfo monoIOSyncMethod;

			/// <inheritdoc/>
			/// <exception cref="NotSupportedException">
			/// If the current platform is not supported. The support of
			/// fsync operations under UNIX-like platforms (Linux, *BSD, etc.)
			/// is done by a call to Mono: if the project was not compiled
			/// with this it will throw this exception by default.
			/// </exception>
			public void Sync(FileStream stream) {
#if !MONO
				throw new NotSupportedException();
#else
				try {
					/*
					if (Mono.Unix.Native.Stdlib.fflush(stream.Handle) != 0)
						Mono.Unix.UnixMarshal.ThrowExceptionForLastError();
					*/
					if (monoIOSyncMethod == null)
						throw new SyncFailedException();
					
					int erro = 0;
					monoIOSyncMethod.Invoke(null, new object[] {stream.Handle, erro});
					if (erro != 0)
						throw new SyncFailedException();
				} catch(Exception) {
					throw new SyncFailedException();
				}
#endif
			}			
		}
#endregion

#region MacOSXFSync
		/// <summary>
		/// Synchronizes a <see cref="FileStream"/> with an underlying
		/// MacOS X platform.
		/// </summary>
		private sealed class MacOSXFSync : IFSync {
			/// <inheritdoc/>
			/// <exception cref="NotSupportedException">
			/// If the current platform is not supported. The support of
			/// fsync operations under MacOS X platform is done by a call 
			/// to Mono: if the project was not compiled with this it will
			/// throw this exception by default.
			/// </exception>
			public void Sync(FileStream stream) {
				throw new NotSupportedException();
			}
		}
#endregion
	}
}