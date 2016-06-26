package com.fusion.partitioner;

/**
 * Hash Partitioner Class - Defaults to this partitioner if 
 * nothing specified by the user.
 * 
 * @author KanthKumar
 *
 */
public class HashPartitioner implements Partitioner{

	private static final long serialVersionUID = 1L;

	@Override
	public Integer partitionLogic(Object element, int partitionCount) {
		System.out.println(element +" :"+ (element.hashCode() & Integer.MAX_VALUE) % partitionCount);
		return (element.hashCode() & Integer.MAX_VALUE) % partitionCount;
	}
		
}
