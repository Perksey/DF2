// Sourced from Umbraco.Core under the MIT license.

using System.Collections.Generic;
using System;
using System.Collections.ObjectModel;
using System.Linq;

namespace Ultz.DF2
{
    /// <summary>
    /// An ObservableDictionary
    /// </summary>
    /// <remarks>
    /// Assumes that the key will not change and is unique for each element in the collection.
    /// Collection is not thread-safe, so calls should be made single-threaded.
    /// </remarks>
    internal class ValueDictionary : ObservableCollection<IValue>, IValueDictionary, IDictionary<string, IValue>
    {
        protected Dictionary<string, int> Indices { get; }
        protected Func<IValue, string> KeySelector { get; }

        /// <summary>
        /// Create new ObservableDictionary
        /// </summary>
        /// <param name="keySelector">Selector function to create key from value</param>
        /// <param name="equalityComparer">The equality comparer to use when comparing keys, or null to use the default comparer.</param>
        public ValueDictionary(Func<IValue, string> keySelector, IEqualityComparer<string> equalityComparer = null)
        {
            KeySelector = keySelector ?? throw new ArgumentException(nameof(keySelector));
            Indices = new Dictionary<string, int>(equalityComparer);
        }

        #region Protected Methods

        protected override void InsertItem(int index, IValue item)
        {
            var key = KeySelector(item);
            if (Indices.ContainsKey(key))
                throw new ArgumentException($"An element with the same key '{key}' already exists in the dictionary.",
                    nameof(item));

            if (index != Count)
            {
                foreach (var k in Indices.Keys.Where(k => Indices[k] >= index).ToList())
                {
                    Indices[k]++;
                }
            }

            base.InsertItem(index, item);
            Indices[key] = index;
        }

        protected override void ClearItems()
        {
            base.ClearItems();
            Indices.Clear();
        }

        protected override void RemoveItem(int index)
        {
            var item = this[index];
            var key = KeySelector(item);

            base.RemoveItem(index);

            Indices.Remove(key);

            foreach (var k in Indices.Keys.Where(k => Indices[k] > index).ToList())
            {
                Indices[k]--;
            }
        }

        #endregion

        public bool ContainsKey(string key)
        {
            return Indices.ContainsKey(key);
        }

        /// <summary>
        /// Gets or sets the element with the specified key.  If setting a new value, new value must have same key.
        /// </summary>
        /// <param name="key">Key of element to replace</param>
        /// <returns></returns>
        public IValue this[string key]
        {
            get => this[Indices[key]];
            set
            {
                //confirm key matches
                if (!KeySelector(value).Equals(key))
                    throw new InvalidOperationException("Key of new value does not match.");

                if (!Indices.ContainsKey(key))
                {
                    Add(value);
                }
                else
                {
                    this[Indices[key]] = value;
                }
            }
        }

        /// <summary>
        /// Replaces element at given key with new value.  New value must have same key.
        /// </summary>
        /// <param name="key">Key of element to replace</param>
        /// <param name="value">New value</param>
        ///
        /// <exception cref="InvalidOperationException"></exception>
        /// <returns>False if key not found</returns>
        public bool Replace(string key, IValue value)
        {
            if (!Indices.ContainsKey(key)) return false;

            //confirm key matches
            if (!KeySelector(value).Equals(key))
                throw new InvalidOperationException("Key of new value does not match.");

            this[Indices[key]] = value;
            return true;
        }

        public void ReplaceAll(IEnumerable<IValue> values)
        {
            if (values == null) throw new ArgumentNullException(nameof(values));

            Clear();

            foreach (var value in values)
            {
                Add(value);
            }
        }

        public bool Remove(string key)
        {
            if (!Indices.ContainsKey(key)) return false;

            RemoveAt(Indices[key]);
            return true;
        }

        /// <summary>
        /// Allows us to change the key of an item
        /// </summary>
        /// <param name="currentKey"></param>
        /// <param name="newKey"></param>
        public void ChangeKey(string currentKey, string newKey)
        {
            if (!Indices.ContainsKey(currentKey))
            {
                throw new InvalidOperationException(
                    $"No item with the key '{currentKey}' was found in the dictionary.");
            }

            if (ContainsKey(newKey))
            {
                throw new ArgumentException(
                    $"An element with the same key '{newKey}' already exists in the dictionary.", nameof(newKey));
            }

            var currentIndex = Indices[currentKey];

            Indices.Remove(currentKey);
            Indices.Add(newKey, currentIndex);
        }

        #region IDictionary and IReadOnlyDictionary implementation

        public bool TryGetValue(string key, out IValue val)
        {
            if (Indices.TryGetValue(key, out var index))
            {
                val = this[index];
                return true;
            }

            val = default;
            return false;
        }

        /// <summary>
        /// Returns all keys
        /// </summary>
        public IEnumerable<string> Keys => Indices.Keys;

        /// <summary>
        /// Returns all values
        /// </summary>
        public IEnumerable<IValue> Values => base.Items;

        ICollection<string> IDictionary<string, IValue>.Keys => Indices.Keys;

        //this will never be used
        ICollection<IValue> IDictionary<string, IValue>.Values => Values.ToList();

        bool ICollection<KeyValuePair<string, IValue>>.IsReadOnly => false;

        IEnumerator<KeyValuePair<string, IValue>> IEnumerable<KeyValuePair<string, IValue>>.GetEnumerator()
        {
            foreach (var i in Values)
            {
                var key = KeySelector(i);
                yield return new KeyValuePair<string, IValue>(key, i);
            }
        }

        void IDictionary<string, IValue>.Add(string key, IValue value)
        {
            Add(value);
        }

        void ICollection<KeyValuePair<string, IValue>>.Add(KeyValuePair<string, IValue> item)
        {
            Add(item.Value);
        }

        bool ICollection<KeyValuePair<string, IValue>>.Contains(KeyValuePair<string, IValue> item)
        {
            return ContainsKey(item.Key);
        }

        void ICollection<KeyValuePair<string, IValue>>.CopyTo(KeyValuePair<string, IValue>[] array, int arrayIndex)
        {
            throw new NotImplementedException();
        }

        bool ICollection<KeyValuePair<string, IValue>>.Remove(KeyValuePair<string, IValue> item)
        {
            return Remove(item.Key);
        }

        #endregion
    }
}