import Sidebar from "../components/sidebar";
import { useEffect, useState } from "react";
import axios from "axios";

const ALSImplicit = () => {
  const [header, setHeader] = useState([] as string[]);
  const [data, setData] = useState([] as string[][]);

  const [cursor, setCursor] = useState([0, 25]);

  useEffect(() => {
    // Fetch a CSV file using axios
    axios.get("/als-implicit.csv").then((response) => {
      const csv = response.data;
      const lines = csv.split(/\r?\n/);
      setHeader(lines[0].split(","));
      console.log(lines);

      let finalData = [] as any;

      lines.forEach((line: any, index: any) => {
        if (index === 0) return;

        finalData = [...finalData, line.split(",")];
      });
      setData(finalData);
    });
  }, []);

  return (
    <div className='view'>
      <Sidebar />
      <div className='model'>
        <div className='model-info'>
          <span className='model-title'>ALS Implicit</span>
          <div className='model-specs'>
            <div className='model-spec'>
              <span className='title'>RMSE</span>
              <span className='content'>-</span>
            </div>
            <div className='model-spec'>
              <span className='title'>FACTOR / REGULARIZATION / ITERATIONS</span>
              <span className='content'>- / - / -</span>
            </div>
            <div className='model-spec'>
              <span className='title'>Final Score</span>
              <span className='content'>336</span>
            </div>
          </div>
        </div>
        <div className='model-content'>
          <div className='header'>
            {header.map((column) => {
              return <span className='column'>{column}</span>;
            })}
          </div>
          <div className='entries'>
            {data.length > 1 ? (
              data.slice(cursor[0], cursor[1]).map((line) => (
                <div className='entry'>
                  {line.map((line, index) => {
                    return <span key={index}>{line}</span>;
                  })}
                </div>
              ))
            ) : (
              <span className='loading'> LOADING</span>
            )}
          </div>
        </div>

        <div className='pagination'>
          <div
            className='backward'
            onClick={() => {
              setCursor([Math.max(cursor[0] - 25, 0), Math.max(cursor[1] - 25, 25)]);
            }}>
            Backwards
          </div>
          <span>{cursor[0] + " - " + cursor[1]}</span>
          <div
            className='forward'
            onClick={() => {
              setCursor([cursor[0] + 25, cursor[1] + 25]);
            }}>
            Forward
          </div>
        </div>
      </div>
    </div>
  );
};
export default ALSImplicit;
