/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.iv2;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Timer;
import java.util.TimerTask;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltdb.messaging.InitiateResponseMessage;

public class ShortCircuitReadLog
{
    VoltLogger tmLog = new VoltLogger("TM");
    public final static long MAX_WAITING_TIME = 10; // 10 millis

    // Initialize to Long MAX_VALUE to prevent feeding a newly joined node
    // transactions it should never have seen
    long m_lastSafeTruncationHandle = Long.MAX_VALUE;

    static class Item
    {
        private final InitiateResponseMessage m_message;

        Item (InitiateResponseMessage message) {
            m_message = message;
        }

        public final InitiateResponseMessage getMessage() {
            return m_message;
        }

        public boolean shouldTruncate(long handle) {
            if (m_message.getSpHandle() <= handle) {
                return true;
            }
            return false;
        }
    }

    final Deque<Item> m_responseMessageQueue;
    Mailbox m_mailbox;

    class CheckToRelease extends TimerTask {
        @Override
        public void run() {
            if (m_mailbox == null) {
                return;
            }

            truncate();
        }
    }

    ShortCircuitReadLog(Mailbox mailbox)
    {
        m_responseMessageQueue = new ArrayDeque<Item>();
        m_mailbox = mailbox;
        Timer timer = new Timer();
        timer.schedule(new CheckToRelease(), 0, 10);
    }

    // Offer a new message.
    public void offer(InitiateResponseMessage msg, Mailbox mailbox, long handle)
    {
        m_lastSafeTruncationHandle = msg.getSpHandle();
        m_mailbox = mailbox;

        if (handle >= m_lastSafeTruncationHandle || handle == Long.MIN_VALUE) {
            // FAST / SAFE_1 does not increase the txn id
            m_mailbox.send(msg.getInitiatorHSId(), msg);
            return;
        }
        m_responseMessageQueue.add(new Item(msg));

        return;
    }

    // trim unnecessary log messages.
    // This will release the message to clients if the message includes a truncation hint.
    public void release(Mailbox mailbox, long handle)
    {
        m_mailbox = mailbox;
        m_lastSafeTruncationHandle = handle;
        // MIN value means no work to do, is a startup condition
        if (handle == Long.MIN_VALUE) {
            return;
        }

        truncate();
    }

    private void truncate() {
        Item item = null;
        while ((item = m_responseMessageQueue.peek()) != null) {
            if (item.shouldTruncate(m_lastSafeTruncationHandle)) {
                InitiateResponseMessage msg = item.getMessage();
                m_mailbox.send(msg.getInitiatorHSId(), msg);
                m_responseMessageQueue.poll();
            } else {
                break;
            }
        }
    }
}
