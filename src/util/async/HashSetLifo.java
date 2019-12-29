/**
 * Ono Project
 *
 * File:         HashSetCache.java
 * RCS:          $Id: HashSetLifo.java,v 1.1 2007/02/01 16:52:19 drc915 Exp $
 * Description:  HashSetCache class (see below)
 * Author:       David Choffnes
 *               Northwestern Systems Research Group
 *               Department of Computer Science
 *               Northwestern University
 * Created:      Jan 22, 2007 at 9:50:40 AM
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

import java.util.Iterator;
import java.util.LinkedHashSet;

/**
 * @author David Choffnes &lt;drchoffnes@cs.northwestern.edu&gt;
 *
 * The HashSetCache class implements a fixed-size cache. The oldest 
 * entry is removed when the capacity is exceeded.
 */
public class HashSetLifo<E> extends LinkedHashSet<E> {
	
	private int maxEntries = 100;
	Iterator it = null;

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public HashSetLifo(int capacity){
		super();
		maxEntries = capacity;	
	}

	public HashSetLifo() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean add(E o) {
		synchronized(this){
			if (this.size()==maxEntries){
				return false;
			}
			return super.add(o);
		}
	}
	
	
	
}
