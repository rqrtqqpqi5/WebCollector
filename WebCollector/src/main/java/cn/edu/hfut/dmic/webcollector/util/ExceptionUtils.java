/*
 * Copyright (C) 2017 hu
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
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package cn.edu.hfut.dmic.webcollector.util;

/**
 * @author hu
 */
public class ExceptionUtils {
    public static void fail(String message) {
        throw new RuntimeException(message);
    }

    public static void fail(String message, Throwable cause) {
        throw new RuntimeException(message, cause);
    }

    public static void fail(Throwable cause) {
        throw new RuntimeException(cause);
    }

    public static void fail() {
        throw new RuntimeException();
    }
}
