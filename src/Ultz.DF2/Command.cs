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
        /// 1 operand of type null-terminated string.
        /// </summary>
        Group,
        
        /// <summary>
        /// Adds or edits a value within the current group.
        /// 3 operands of types byte, null-terminated string, and a value of variable length determined by the first
        /// byte.
        /// </summary>
        Value,
        
        /// <summary>
        /// Removes a value within the current group.
        /// 1 operand of type null-terminated string.
        /// </summary>
        Remove,
        
        /// <summary>
        /// Assigns a handle to a value using a path of its names. The path may be absolute or relative to the current
        /// group. If the first operand is an empty string, the handle is invalidated.
        /// 2 operands of types null-terminated string and compressed uint.
        /// </summary>
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
        GroupByHandle,
    }
}