import { useState } from 'react';

export default function AIInsight() {
  const [insight, setInsight] = useState('');
  const [loading, setLoading] = useState(false);

  async function fetchInsight() {
    setLoading(true);
    try {
      const res = await fetch('/api/ai-insight');
      const data = await res.json();
      setInsight(data.insight);
    } catch {
      setInsight('Không thể kết nối AI. Vui lòng thử lại.');
    } finally {
      setLoading(false);
    }
  }

  return (
    <div style={{
      background: 'var(--surface)', border: '1px solid var(--border)',
      borderRadius: 10, padding: 18, marginBottom: 20,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
        <div style={{ fontSize: 11, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: '0.8px', fontFamily: 'var(--mono)' }}>
          🤖 AI Insight — Phân tích doanh thu
        </div>
        <button onClick={fetchInsight} disabled={loading} style={{
          background: '#1a2540', border: '1px solid #1e3a5f',
          color: '#22d3ee', padding: '5px 14px', borderRadius: 6,
          cursor: loading ? 'not-allowed' : 'pointer', fontSize: 12,
          fontFamily: 'var(--mono)',
        }}>
          {loading ? 'Đang phân tích...' : '✨ Phân tích ngay'}
        </button>
      </div>

      {insight && (
        <p style={{ fontSize: 14, color: 'var(--text)', lineHeight: 1.7, borderLeft: '3px solid #3b82f6', paddingLeft: 12, whiteSpace: 'pre-line' }}>
          {insight}
        </p>
      )}

      {!insight && !loading && (
        <p style={{ fontSize: 13, color: 'var(--muted)', fontStyle: 'italic' }}>
          Nhấn "Phân tích ngay" để nhận nhận xét từ AI về tình hình kinh doanh.
        </p>
      )}
    </div>
  );
}
