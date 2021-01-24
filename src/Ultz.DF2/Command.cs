namespace Ultz.DF2
{
    public enum Command : byte
    {
        /// <summary>
        /// Ends the document.
        /// No operands.
        /// </summary>
        End,

        /// <summary>
        /// Enters a group with the given path, or creates one if one doesn't exist. 
        /// 1 operand of type compressed-length-preceded string.
        /// </summary>
        /// <remarks>
        /// Compressed length preceded strings refer to a string where:
        /// - first, the length of the string's UTF8 bytes are written to the stream as a compressed 32-bit unsigned
        /// integer
        /// - after, the string's UTF8 bytes are written
        /// </remarks>
        Group,

        /// <summary>
        /// Adds or edits a value within the current group.
        /// 3 operands of types byte, compressed-length-preceded string, and a value of variable length determined by
        /// the first byte.
        /// </summary>
        /// <remarks>
        /// Compressed length preceded strings refer to a string where:
        /// - first, the length of the string's UTF8 bytes are written to the stream as a compressed 32-bit unsigned
        /// integer
        /// - after, the string's UTF8 bytes are written
        /// </remarks>
        Value,

        /// <summary>
        /// Removes a value within the current group.
        /// 1 operand of type compressed-length-preceded string.
        /// </summary>
        /// <remarks>
        /// Compressed length preceded strings refer to a string where:
        /// - first, the length of the string's UTF8 bytes are written to the stream as a compressed 32-bit unsigned
        /// integer
        /// - after, the string's UTF8 bytes are written
        /// </remarks>
        Remove,

        /// <summary>
        /// Assigns a handle to a value using a path of its names. The path may be absolute or relative to the current
        /// group. If the first operand is an empty string, the handle is invalidated.
        /// 2 operands of types compressed-length-preceded string and compressed uint.
        /// </summary>
        /// <remarks>
        /// Compressed length preceded strings refer to a string where:
        /// - first, the length of the string's UTF8 bytes are written to the stream as a compressed 32-bit unsigned
        /// integer
        /// - after, the string's UTF8 bytes are written
        /// </remarks>
        Handle,

        /// <summary>
        /// Edits an existing value by using its assigned handle.
        /// 2 operands of type compressed uint and a value of variable length determined by the created value.
        /// </summary>
        EditValueByHandle,

        /// <summary>
        /// Enters a group with the given handle.
        /// 1 operand of type compressed uint
        /// </summary>
        GroupByHandle
    }
}