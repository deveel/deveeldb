// 
//  Copyright 2010-2017 Deveel
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

namespace Deveel.Data.Events {
	/// <summary>
	/// The default base implementation of an event source
	/// that provides contextual information
	/// </summary>
	/// <remarks>
	/// The metadata of the source are collected at construction:
	/// implementations of this object will override <see cref="GetMetadata"/>
	/// to populate the data that will be stored and carried along
	/// </remarks>
	public abstract class EventSource : IEventSource {
		private Dictionary<string, object> sourceMetadata;

		/// <summary>
		/// Constructs a new <see cref="EventSource"/> object
		/// that has no parent.
		/// </summary>
		protected EventSource() 
			: this(null) {
		}

		/// <summary>
		/// Constructs a new <seealso cref="EventSource"/>
		/// that is the child of the given source
		/// </summary>
		/// <param name="parentSource">The parent source of this object</param>
		protected EventSource(IEventSource parentSource) {
			ParentSource = parentSource;
			EnsureMetadata();
		}

		IEventSource IEventSource.ParentSource => ParentSource;

		/// <summary>
		/// Gets a reference to an optional parent source.
		/// </summary>
		protected IEventSource ParentSource { get; }

		public static EventSource Environment => new EnvironmentEventSource();

		private void EnsureMetadata() {
			var meta = new Dictionary<string, object>();
			GetMetadata(meta);

			sourceMetadata = new Dictionary<string, object>(meta);
		}

		IDictionary<string, object> IEventSource.Metadata => sourceMetadata;

		/// <summary>
		/// When overridden by an implementing class, this method
		/// populates the provided dictionary with the metadata that 
		/// describe the source of events
		/// </summary>
		/// <param name="metadata">The dictionary that will be populated
		/// with the metadata information</param>
		protected virtual void GetMetadata(Dictionary<string, object> metadata) {
		}
	}
}