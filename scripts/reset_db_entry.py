import sqlite3
conn = sqlite3.connect('data/trading.db')
conn.row_factory = sqlite3.Row
conn.execute("DELETE FROM orders")
conn.execute("DELETE FROM positions_stat_arb")
conn.commit()
print("DB reset complete")
conn.close()
