import { useState, useEffect } from 'react';
import { Bar, Line } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend
} from 'chart.js';

// Register ChartJS components
ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend
);

interface ScoreData {
  bucket: string;
  count: number;
}

interface PassRateData {
  task: string;
  avg_score: number;
  attempts: number;
}

interface TimelineData {
  date: string;
  submissions: number;
}

interface DashboardProps {
  token: string;
}

function Dashboard({ token }: DashboardProps) {
  const [selectedLab, setSelectedLab] = useState('lab-04');
  const [scoreData, setScoreData] = useState<ScoreData[]>([]);
  const [passRateData, setPassRateData] = useState<PassRateData[]>([]);
  const [timelineData, setTimelineData] = useState<TimelineData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const labs = ['lab-01', 'lab-02', 'lab-03', 'lab-04', 'lab-05'];

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      setError(null);
      
      try {
        const headers = {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        };

        const baseUrl = import.meta.env.VITE_API_URL;

        // Fetch all three endpoints in parallel
        const [scoresRes, passRatesRes, timelineRes] = await Promise.all([
          fetch(`${baseUrl}/analytics/scores?lab=${selectedLab}`, { headers }),
          fetch(`${baseUrl}/analytics/pass-rates?lab=${selectedLab}`, { headers }),
          fetch(`${baseUrl}/analytics/timeline?lab=${selectedLab}`, { headers })
        ]);

        if (!scoresRes.ok || !passRatesRes.ok || !timelineRes.ok) {
          throw new Error('Failed to fetch analytics data');
        }

        const scores = await scoresRes.json();
        const passRates = await passRatesRes.json();
        const timeline = await timelineRes.json();

        setScoreData(scores);
        setPassRateData(passRates);
        setTimelineData(timeline);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'An error occurred');
      } finally {
        setLoading(false);
      }
    };

    if (token) {
      fetchData();
    }
  }, [token, selectedLab]);

  // Prepare chart data
  const barChartData = {
    labels: scoreData.map(item => item.bucket),
    datasets: [
      {
        label: 'Number of Submissions',
        data: scoreData.map(item => item.count),
        backgroundColor: 'rgba(75, 192, 192, 0.6)',
        borderColor: 'rgba(75, 192, 192, 1)',
        borderWidth: 1,
      },
    ],
  };

  const lineChartData = {
    labels: timelineData.map(item => item.date),
    datasets: [
      {
        label: 'Submissions per Day',
        data: timelineData.map(item => item.submissions),
        fill: false,
        borderColor: 'rgb(75, 192, 192)',
        tension: 0.1,
      },
    ],
  };

  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'top' as const,
      },
    },
  };

  if (loading) {
    return <div className="dashboard-loading">Loading charts...</div>;
  }

  if (error) {
    return <div className="dashboard-error">Error: {error}</div>;
  }

  return (
    <div className="dashboard">
      <header className="dashboard-header">
        <h2>Analytics Dashboard</h2>
        <div className="lab-selector">
          <label htmlFor="lab-select">Select Lab: </label>
          <select
            id="lab-select"
            value={selectedLab}
            onChange={(e) => setSelectedLab(e.target.value)}
          >
            {labs.map((lab) => (
              <option key={lab} value={lab}>
                {lab.toUpperCase()}
              </option>
            ))}
          </select>
        </div>
      </header>

      <div className="charts-container">
        {scoreData.length > 0 && (
          <div className="chart-card">
            <h3>Score Distribution</h3>
            <div className="chart-wrapper">
              <Bar data={barChartData} options={chartOptions} />
            </div>
          </div>
        )}

        {timelineData.length > 0 && (
          <div className="chart-card">
            <h3>Submission Timeline</h3>
            <div className="chart-wrapper">
              <Line data={lineChartData} options={chartOptions} />
            </div>
          </div>
        )}

        {passRateData.length > 0 && (
          <div className="chart-card">
            <h3>Task Pass Rates</h3>
            <table className="pass-rates-table">
              <thead>
                <tr>
                  <th>Task</th>
                  <th>Average Score</th>
                  <th>Attempts</th>
                </tr>
              </thead>
              <tbody>
                {passRateData.map((item) => (
                  <tr key={item.task}>
                    <td>{item.task}</td>
                    <td>{item.avg_score.toFixed(1)}%</td>
                    <td>{item.attempts}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

export default Dashboard;
