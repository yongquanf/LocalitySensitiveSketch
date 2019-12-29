/**
 * Ono Project
 *
 * File:         HashMapCache.java
 * RCS:          $Id: HashMapCache.java,v 1.1 2007/02/01 16:52:19 drc915 Exp $
 * Description:  HashMapCache class (see below)
 * Author:       David Choffnes
 *               Northwestern Systems Research Group
 *               Department of Computer Science
 *               Northwestern University
 * Created:      Jan 22, 2007 at 9:56:15 AM
 * Language:     Java
 * Package:      edu.northwestern.ono.util
 * Status:       Experimental (Do Not Distribute)
 *
 * (C) Copyright 2007, Northwestern University, all rights reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA
 */
package util.async;

import java.util.LinkedHashMap;

/**
 * @author David Choffnes &lt;drchoffnes@cs.northwestern.edu&gt;
 *
 * The HashMapCache class implements a fixed-size cache. Oldest entries 
 * are replaced first.
 */
public class HashMapCache<T, V> extends LinkedHashMap<T, V> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3138040523261432199L;
	private int maxEntries = 100;
	
	public HashMapCache(int capacity){
		maxEntries = capacity;
	}

	public HashMapCache() {
		super();
	}

	@Override
	protected boolean removeEldestEntry(java.util.Map.Entry eldest) {
		
		return size() > maxEntries;
	}
	
	
}
