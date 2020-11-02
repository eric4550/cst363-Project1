package disk_store;

import java.util.ArrayList;
import java.util.List;

/**
 * An ordered index.  Duplicate search key values are allowed,
 * but not duplicate index table entries.  In DB terminology, a
 * search key is not a superkey.
 * 
 * A limitation of this class is that only single integer search
 * keys are supported.
 *
 */


public class OrdIndex implements DBIndex {
	
	private class Entry {
		int key;
		ArrayList<BlockCount> blocks;
	}
	
	private class BlockCount {
		int blockNo;
		int count;
	}
	
	ArrayList<Entry> entries;
	int size = 0;
	
	/**
	 * Create an new ordered index.
	 */
	public OrdIndex() {
		entries = new ArrayList<>();
	}
	
	@Override
	public List<Integer> lookup(int key) {
		// binary search of entries arraylist
		// return list of block numbers (no duplicates). 
		// if key not found, return empty list
		int first = 0;
		int last = entries.size()-1;
		List<Integer> blockNums = new ArrayList<>();

		while(first <= last) {
			int middle = (first + last) / 2;
			if(entries.get(middle).key == key) {
				for(BlockCount t : entries.get(middle).blocks) {
					if(!blockNums.contains(t.blockNo)) {
						blockNums.add(t.blockNo);
					}
				}
				return blockNums;
			} else if(entries.get(middle).key > key) {
				last = middle - 1;
			} else {
				first = middle + 1;
			}
		}
		return blockNums;
	}
	
	@Override
	public void insert(int key, int blockNum) {
		boolean foundKey = false;
		int position = -1;
		for(int i = 0; i < entries.size(); i++) {
			if(entries.get(i).key == key) {
				foundKey = true;
				position = i;
				for (BlockCount b : entries.get(i).blocks) {
					if (b.blockNo == blockNum) {
						b.count++;
						return;
					}
				}
			}
		}

		BlockCount b = new BlockCount();
		b.count = 1;
		b.blockNo = blockNum;

		if(!foundKey) {
			Entry e = new Entry();
			e.key = key;
			e.blocks = new ArrayList<>();
			e.blocks.add(b);
			entries.add(e);
			size++;
		} else {
			entries.get(position).blocks.add(b);
			size++;
		}


	}

	@Override
	public void delete(int key, int blockNum) {
		List<Integer> result = lookup(key);
		// lookup key
		if(result.isEmpty()) {
			return;
		}

		for(Entry e : entries) {
			if(e.key == key) {
				if(e.blocks.isEmpty()) {
					entries.remove(e);
					size--;
					return;
				}
				for(BlockCount b : e.blocks) {
					if(b.blockNo == blockNum) {
						b.count--;
						if(b.count == 0) {
							e.blocks.remove(b);
							size--;
						}
						return;
					}
				}
			}
		}
	}
	
	/**
	 * Return the number of entries in the index
	 * @return
	 */
	public int size() {
		return size;
		// you may find it useful to implement this
		
	}
	
	@Override
	public String toString() {
		throw new UnsupportedOperationException();
	}
}