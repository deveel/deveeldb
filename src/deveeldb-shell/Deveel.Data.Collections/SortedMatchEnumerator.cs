using System;
using System.Collections;

namespace Deveel.Collections {
	/// <summary>
	/// An <see cref="IEnumerator">enumerator</see> returning end-truncated 
	/// matching values from a sorted list.
	/// </summary>
	/// <remarks>
	/// This IEnumerator is initialized with a sorted ISet, sorted 
	/// IDictionary or another IEnumerator that must be placed at the 
	/// beginning of the matching area of a sorted set.
	/// <para>
	/// This IEnumerator is commonly used for TAB-completion.
	/// </para>
	/// </remarks>
	public class SortedMatchEnumerator : IEnumerator {
		#region ctor
		public SortedMatchEnumerator(string partialMatch, IEnumerator/*<String>*/ en) {
			this.partialMatch = partialMatch;
			this.en = en;
		}

		public SortedMatchEnumerator(string partialMatch, ISortedSet/*<String>*/ set)
			: this(partialMatch, set.TailSet(partialMatch).GetEnumerator()) {
		}

		public SortedMatchEnumerator(string partialMatch, ISortedDictionary/*<String>*/ dictionary)
			: this(partialMatch, dictionary.TailDictionary(partialMatch).Keys.GetEnumerator()) {
		}
		#endregion

		#region Fields
		private readonly IEnumerator en;
		private readonly string partialMatch;

		private string prefix;
		private string suffix;

		// the current match
		private string current;
		#endregion

		#region Properties
		public string Prefix {
			get { return prefix; }
			set { prefix = value; }
		}

		public string Suffix {
			get { return suffix; }
			set { suffix = value; }
		}

		public object Current {
			get {
				string result = current;
				if (prefix != null)
					result = prefix + result;
				if (suffix != null)
					result = result + suffix;
				return result;
			}
		}
		#endregion

		#region Protected Methods
		protected virtual bool Exclude(string current) {
			return false;
		}
		#endregion

		#region Public Methods
		public bool MoveNext() {
			while (en.MoveNext()) {
				current = (string)en.Current;
				if (current.Length == 0)
					continue;
				if (!current.StartsWith(partialMatch))
					return false;
				if (Exclude(current))
					continue;
				return true;
			}
			return false;
		}

		public void Reset() {
			current = null;
			en.Reset();
		}
		#endregion
	}
}