using System;
using System.Collections;

namespace Deveel.Collections {
	[Serializable]
	public sealed class TreeSet : ISortedSet, ICloneable {
		#region ctor
		public TreeSet() :
			this((IComparer)null) {
		}

		public TreeSet(IComparer comparer) {
			dictionary = new TreeDictionary(comparer);
		}

		public TreeSet(ICollection collection) :
			this() {
			AddRange(collection);
		}

		private TreeSet(ISortedDictionary dictionary) {
			this.dictionary = dictionary;
		}
		#endregion

		#region Fields
		private ISortedDictionary dictionary;
		#endregion

		#region ISortedSet Members
		public IComparer Comparer {
			get { return dictionary.Comparer; }
		}

		public object First {
			get { return dictionary.FirstKey; }
		}

		public object Last {
			get { return dictionary.LastKey; }
		}

		public ISortedSet GetHeadList(object endElement) {
			return new TreeSet(dictionary.GetHeadDictionary(endElement));
		}

		public ISortedSet GetSubList(object startElement, object endElement) {
			return new TreeSet(dictionary.GetSubDictionary(startElement, endElement));
		}

		public ISortedSet TailSet(object startElement) {
			return new TreeSet(dictionary.TailDictionary(startElement));
		}

		#endregion

		#region IList Members

		public bool IsEmpty {
			get { return dictionary.Count != 0; }
		}

		public void Add(object obj) {
			dictionary.Add(obj, "");
		}

		public void AddRange(ICollection c) {
			IEnumerator en = c.GetEnumerator();
			while (en.MoveNext())
				dictionary.Add(en.Current, "");
		}

		public void Remove(object obj) {
			dictionary.Remove(obj);
		}

		public override bool Equals(object obj) {
			return (obj == this || (obj is IList && ((IList)obj).Count == Count &&
				ContainsRange((ICollection)obj)));
		}

		public override int GetHashCode() {
			IEnumerator en = GetEnumerator();
			int hash = 0;
			while (en.MoveNext())
				hash += en.Current.GetHashCode();
			return hash;
		}

		public void Clear() {
			dictionary.Clear();
		}

		public bool Contains(object obj) {
			return dictionary.Contains(obj);
		}

		public bool ContainsRange(ICollection c) {
			IEnumerator en = c.GetEnumerator();
			while (en.MoveNext())
				if (!Contains(en.Current))
					return false;
			return true;
		}

		public object[] ToArray() {
			IEnumerator en = GetEnumerator();
			int size = Count;
			object[] array = new Object[size];
			for (int pos = 0; pos < size && en.MoveNext(); pos++)
				array[pos] = en.Current;
			return array;
		}

		public Array ToArray(Type type) {
			int size = Count;
			Array array = Array.CreateInstance(type, size);

			IEnumerator en = GetEnumerator();
			for (int pos = 0; pos < size && en.MoveNext(); pos++)
				array.SetValue(en.Current, pos);

			return array;
		}

		#endregion

		#region ICollection Members

		public void CopyTo(Array array, int index) {
			throw new NotSupportedException();
		}

		public int Count {
			get { return dictionary.Count; }
		}

		public bool IsSynchronized {
			get { return false; }
		}

		public object SyncRoot {
			get { return this; }
		}

		#endregion

		#region IEnumerable Members

		public IEnumerator GetEnumerator() {
			return dictionary.Keys.GetEnumerator();
		}

		#endregion

		#region ICloneable Members

		public object Clone() {
			TreeSet copy = null;
			copy = (TreeSet)base.MemberwiseClone();
			// Map may be either TreeMap or TreeMap.SubMap, hence the ugly casts.
			copy.dictionary = (TreeDictionary)((ICloneable)dictionary).Clone();
			return copy;
		}

		#endregion
	}
}